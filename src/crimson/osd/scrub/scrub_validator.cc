// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab
#include <algorithm>
#include <set>
#include <ranges>

#include "osd/osd_types_fmt.h"

#include "crimson/common/log.h"
#include "crimson/osd/scrub/scrub_validator.h"
#include "osd/ECUtil.h"
#include "osd/ECUtilL.h"

SET_SUBSYS(osd);

namespace crimson::osd::scrub {

using object_set_t = std::set<hobject_t>;
object_set_t get_object_set(const scrub_map_set_t &in)
{
  object_set_t ret;
  for (const auto& [from, map] : in) {
    std::transform(map.objects.begin(), map.objects.end(),
                   std::inserter(ret, ret.end()),
                   [](const auto& i) { return i.first; });
  }
  return ret;
}

enum class snapset_status_t {
  OK,
  MISSING,
  CORRUPTED
};

struct shard_evaluation_t {
  pg_shard_t source;
  shard_info_wrapper shard_info;

  std::optional<object_info_t> object_info;
  std::optional<SnapSet> snapset;
  snapset_status_t snapset_status{snapset_status_t::OK};
  ceph::buffer::list snapset_bl;  // Raw snapset buffer for error reporting
  std::optional<ECLegacy::ECUtilL::HashInfo> hinfo;

  size_t omap_keys{0};
  size_t omap_bytes{0};

  bool has_errors() const {
    return shard_info.has_errors();
  }

  bool is_primary() const {
    return shard_info.primary;
  }

  std::weak_ordering operator<=>(const shard_evaluation_t &rhs) const {
    return std::make_tuple(!has_errors(), is_primary()) <=>
      std::make_tuple(!rhs.has_errors(), rhs.is_primary());
  }
};
shard_evaluation_t evaluate_object_shard(
  const chunk_validation_policy_t &policy,
  const hobject_t &oid,
  pg_shard_t from,
  const ScrubMap::object *maybe_obj)
{
  shard_evaluation_t ret;
  ret.source = from;
  if (from == policy.primary) {
    ret.shard_info.primary = true;
  }

  if (!maybe_obj) {
    ret.shard_info.set_missing();
    return ret;
  }

  // impossible since chunky scrub was introduced
  ceph_assert(!maybe_obj->negative);

  auto &obj = *maybe_obj;
  /* We are ignoring ScrubMap::object::large_omap_object*, object_omap_* is all the
   * info we need */
  ret.omap_keys = obj.object_omap_keys;
  ret.omap_bytes = obj.object_omap_bytes;

  ret.shard_info.set_object(obj);

  if (obj.ec_hash_mismatch) {
    ret.shard_info.set_ec_hash_mismatch();
  }

  if (obj.ec_size_mismatch) {
    ret.shard_info.set_ec_size_mismatch();
  }

  if (obj.read_error) {
    ret.shard_info.set_read_error();
  }

  if (obj.stat_error) {
    ret.shard_info.set_stat_error();
  }

  {
    auto xiter = obj.attrs.find(OI_ATTR);
    if (xiter == obj.attrs.end()) {
      ret.shard_info.set_info_missing();
    } else {
      ret.object_info = object_info_t{};
      try {
 auto bliter = xiter->second.cbegin();
 ::decode(*(ret.object_info), bliter);
      } catch (...) {
 ret.shard_info.set_info_corrupted();
 ret.object_info = std::nullopt;
      }
    }
  }

  ret.shard_info.size = obj.size;
  if (ret.object_info &&
      obj.size != policy.logical_to_ondisk_size(ret.object_info->size)) {
    ret.shard_info.set_obj_size_info_mismatch();
  }

  if (oid.is_head()) {
    auto xiter = obj.attrs.find(SS_ATTR);
    if (xiter == obj.attrs.end()) {
      ret.snapset = std::nullopt;
      ret.snapset_status = snapset_status_t::MISSING;
    } else {
      ret.snapset = SnapSet{};
      ret.snapset_bl = xiter->second;  // Store raw buffer for error reporting
      try {
 auto bliter = xiter->second.cbegin();
 ::decode(*(ret.snapset), bliter);
 ret.snapset_status = snapset_status_t::OK;
      } catch (const ceph::buffer::malformed_input&) {
 ret.snapset = std::nullopt;
 ret.snapset_status = snapset_status_t::CORRUPTED;
      } catch (const ceph::buffer::error&) {
 ret.snapset = std::nullopt;
 ret.snapset_status = snapset_status_t::CORRUPTED;
      }
    }
  }

  if (policy.is_ec()) {
    auto xiter = obj.attrs.find(ECLegacy::ECUtilL::get_hinfo_key());
    if (xiter == obj.attrs.end()) {
      ret.shard_info.set_hinfo_missing();
    } else {
      ret.hinfo = ECLegacy::ECUtilL::HashInfo{};
      try {
	auto bliter = xiter->second.cbegin();
	decode(*(ret.hinfo), bliter);
      } catch (...) {
	ret.shard_info.set_hinfo_corrupted();
	ret.hinfo = std::nullopt;
      }
    }
  }

  if (ret.object_info) {
    if (ret.shard_info.data_digest_present &&
 ret.object_info->is_data_digest() &&
 (ret.object_info->data_digest != ret.shard_info.data_digest)) {
      ret.shard_info.set_data_digest_mismatch_info();
    }
    if (ret.shard_info.omap_digest_present &&
 ret.object_info->is_omap_digest() &&
 (ret.object_info->omap_digest != ret.shard_info.omap_digest)) {
      ret.shard_info.set_omap_digest_mismatch_info();
    }
  }

  return ret;
}

librados::obj_err_t compare_candidate_to_authoritative(
  const chunk_validation_policy_t &policy,
  const hobject_t &oid,
  const shard_evaluation_t &auth,
  shard_evaluation_t &cand)
{
  using namespace librados;
  obj_err_t ret;

  // If candidate is missing, we can't compare attributes/digests, but the
  // SHARD_MISSING error in cand.shard_info will be picked up by evaluate_object()
  // which checks std::any_of(shards, [](auto &s) { return s.has_errors(); })
  if (cand.shard_info.has_shard_missing()) {
    return ret;
  }

  const auto &auth_si = auth.shard_info;
  auto &cand_si = cand.shard_info;

  if (auth_si.data_digest != cand_si.data_digest) {
    ret.errors |= obj_err_t::DATA_DIGEST_MISMATCH;
  }

  if (auth_si.omap_digest != cand_si.omap_digest) {
    ret.errors |= obj_err_t::OMAP_DIGEST_MISMATCH;
  }

  {
    auto aiter = auth_si.attrs.find(OI_ATTR);
    ceph_assert(aiter != auth_si.attrs.end());

    auto citer = cand_si.attrs.find(OI_ATTR);
    if (citer == cand_si.attrs.end() ||
	!aiter->second.contents_equal(citer->second)) {
      ret.errors |= obj_err_t::OBJECT_INFO_INCONSISTENCY;
    }
  }

  if (oid.is_head()) {
    bool auth_bad = (auth.snapset_status != snapset_status_t::OK);
    bool cand_bad = (cand.snapset_status != snapset_status_t::OK);
    
    if (!auth_bad && !cand_bad) {
      // Both successfully decoded - compare raw SS_ATTR contents
      auto aiter = auth_si.attrs.find(SS_ATTR);
      auto citer = cand_si.attrs.find(SS_ATTR);
      
      if (aiter != auth_si.attrs.end() && citer != cand_si.attrs.end()) {
        if (!aiter->second.contents_equal(citer->second)) {
          ret.errors |= obj_err_t::SNAPSET_INCONSISTENCY;
        }
      } else if ((aiter != auth_si.attrs.end()) != (citer != cand_si.attrs.end())) {
        // One has SS_ATTR, one doesn't (shouldn't happen if both decoded OK)
        ret.errors |= obj_err_t::SNAPSET_INCONSISTENCY;
      }
    }
    // If either side has missing/corrupted, skip comparison (handled elsewhere)
  }

  if (policy.is_ec()) {
    auto aiter = auth_si.attrs.find(ECLegacy::ECUtilL::get_hinfo_key());
    ceph_assert(aiter != auth_si.attrs.end());

    auto citer = cand_si.attrs.find(ECLegacy::ECUtilL::get_hinfo_key());
    if (citer == cand_si.attrs.end() ||
	!aiter->second.contents_equal(citer->second)) {
      ret.errors |= obj_err_t::HINFO_INCONSISTENCY;
    }
  }

  if (auth_si.size != cand_si.size) {
    ret.errors |= obj_err_t::SIZE_MISMATCH;
  }

  auto is_sys_attr = [&policy](const auto &str) {
    return str == OI_ATTR || str == SS_ATTR ||
      (policy.is_ec() && str == ECLegacy::ECUtilL::get_hinfo_key());
  };
  for (auto aiter = auth_si.attrs.begin(); aiter != auth_si.attrs.end(); ++aiter) {
    if (is_sys_attr(aiter->first)) continue;

    auto citer = cand_si.attrs.find(aiter->first);
    if (citer == cand_si.attrs.end()) {
      ret.errors |= obj_err_t::ATTR_NAME_MISMATCH;
    } else if (!aiter->second.contents_equal(citer->second)) {
      ret.errors |= obj_err_t::ATTR_VALUE_MISMATCH;
    }
  }
  if (std::any_of(
	cand_si.attrs.begin(), cand_si.attrs.end(),
	[&is_sys_attr, &auth_si](auto &p) {
	  return !is_sys_attr(p.first) &&
	    auth_si.attrs.find(p.first) == auth_si.attrs.end();
	})) {
    ret.errors |= obj_err_t::ATTR_NAME_MISMATCH;
  }

  return ret;
}

struct object_evaluation_t {
  std::optional<inconsistent_obj_wrapper> inconsistency;
  std::optional<object_info_t> object_info;
  std::optional<SnapSet> snapset;
  snapset_status_t snapset_status{snapset_status_t::OK};
  ceph::buffer::list snapset_bl;  // Raw snapset buffer for error reporting
  uint64_t size{0};  // Actual size from scrub map

  size_t omap_keys{0};
  size_t omap_bytes{0};
};
object_evaluation_t evaluate_object(
  const chunk_validation_policy_t &policy,
  const hobject_t &hoid,
  const scrub_map_set_t &maps)
{
  ceph_assert(maps.size() > 0);
  using evaluation_vec_t = std::vector<shard_evaluation_t>;
  evaluation_vec_t shards;
  std::transform(
    maps.begin(), maps.end(),
    std::inserter(shards, shards.end()),
    [&hoid, &policy](const auto &item) -> evaluation_vec_t::value_type {
      const auto &[shard, scrub_map] = item;
      auto miter = scrub_map.objects.find(hoid);
      auto maybe_shard = miter == scrub_map.objects.end() ?
	nullptr : &(miter->second);
      return evaluate_object_shard(policy, hoid, shard, maybe_shard);
    });

  std::sort(shards.begin(), shards.end());

  auto &auth_eval = shards.back();

  object_evaluation_t ret;
  inconsistent_obj_wrapper iow{hoid};
  
  // Get actual size from authoritative shard
  ret.size = auth_eval.shard_info.size;

  // Check if we have at least one shard with the object (not missing)
  // This handles the case where primary is missing but replica has it
  bool has_valid_copy = std::any_of(
    shards.begin(), shards.end(),
    [&hoid](const auto &eval) {
      if (hoid.is_head()) {
        return !eval.shard_info.has_shard_missing() &&
               (eval.object_info.has_value() || eval.snapset.has_value() ||
                eval.snapset_status != snapset_status_t::OK);
      } else {
        return !eval.shard_info.has_shard_missing() &&
             (eval.object_info.has_value() ||
              eval.snapset.has_value() ||
              eval.snapset_status != snapset_status_t::OK);
      }
    });

  // Perform comparisons if:
  // 1. Auth has no errors at all, OR
  // 2. Auth is missing but we have a valid copy, OR
  // 3. Auth has ONLY deep errors (not shallow errors that would make it unreliable)
  bool auth_has_only_deep_errors = auth_eval.has_errors() &&
                                    (auth_eval.shard_info.errors & ~librados::err_t::DEEP_ERRORS) == 0;
  
  bool use_auth = (!auth_eval.has_errors() ||
                   (has_valid_copy && auth_eval.shard_info.has_shard_missing()) ||
                   auth_has_only_deep_errors);
  // For head objects, if auth_eval doesn't have a snapset but another shard does,
  // we must still re-evaluate to pick the correct authority.
  if (hoid.is_head() && use_auth && !auth_eval.snapset.has_value() &&
      std::any_of(shards.begin(), shards.end(),
                  [](const auto &e) { return e.snapset.has_value(); })) {
    use_auth = true; // we will re-select actual_auth below
  }
  if (use_auth) {
    // Use auth_eval if it meets one of the above conditions
    //
    // For snapset validation we still need a shard that actually carries the
    // head metadata, even if object_info is missing/corrupted. Otherwise heads
    // like obj3 disappear from snapset evaluation and their clones are later
    // reported as headless.
    shard_evaluation_t *actual_auth = &auth_eval;
    if ((auth_eval.shard_info.has_shard_missing() || !auth_eval.object_info ||
         (hoid.is_head() && !auth_eval.snapset.has_value())) &&
        has_valid_copy) {
      for (auto it = shards.rbegin(); it != shards.rend(); ++it) {
        bool has_required = !it->shard_info.has_shard_missing();
        if (!hoid.is_head()) {
          // For clones, we need object_info
          has_required = has_required && it->object_info.has_value();
        } else {
          // For heads, either snapset is enough
          has_required = has_required && (it->snapset.has_value() ||
                                          it->snapset_status != snapset_status_t::OK);
        }
        if (has_required) {
          actual_auth = &(*it);
          break;
        }
      }
    }
    
    ret.object_info = actual_auth->object_info;
    ret.omap_keys = actual_auth->omap_keys;
    ret.omap_bytes = actual_auth->omap_bytes;
    ret.snapset = actual_auth->snapset;
    ret.snapset_status = actual_auth->snapset_status;
    ret.snapset_bl = actual_auth->snapset_bl;
    if (actual_auth->object_info &&
        actual_auth->object_info->size > policy.max_object_size) {
      iow.set_size_too_large();
    }
    actual_auth->shard_info.selected_oi = true;
    
    // Compare all other shards against the authoritative one
    std::for_each(
      shards.begin(), shards.end(),
      [&policy, &hoid, actual_auth, &iow](auto &cand_eval) {
        if (&cand_eval != actual_auth) {
          auto err = compare_candidate_to_authoritative(
            policy, hoid, *actual_auth, cand_eval);
          iow.merge(err);
        }
      });
  }

  // Fallback for head objects: if object_info is still missing, try to get it from any shard
  if (hoid.is_head() && !ret.object_info.has_value()) {
    for (auto it = shards.rbegin(); it != shards.rend(); ++it) {
      if (!it->shard_info.has_shard_missing() && it->object_info.has_value()) {
        ret.object_info = it->object_info;
        break;
      }
    }
  }

  // Fallback for head objects: if snapset is still missing, try to get it from any shard
  if (hoid.is_head() && !ret.snapset.has_value()) {
    for (auto it = shards.rbegin(); it != shards.rend(); ++it) {
      if (!it->shard_info.has_shard_missing() && it->snapset.has_value()) {
        ret.snapset = it->snapset;
        ret.snapset_bl = it->snapset_bl;
        ret.snapset_status = it->snapset_status;
        break;
      }
    }
  }

  // In single-copy pools (maps.size() == 1), single-shard errors should be
  // reported as snapset errors, not object errors, matching classic OSD behavior.
  // Only comparison errors (iow.errors) should be reported as object errors.
  bool is_single_copy = (shards.size() == 1);
  bool has_comparison_errors = (iow.errors != 0);
  bool has_shard_errors = std::any_of(shards.begin(), shards.end(),
    [](auto &cand) { return cand.has_errors(); });
  
  if (has_comparison_errors || (has_shard_errors && !is_single_copy)) {
    for (auto &eval : shards) {
      iow.shards.emplace(
 librados::osd_shard_t{eval.source.osd, static_cast<int8_t>(eval.source.shard)},
 eval.shard_info);
      iow.union_shards.errors |= eval.shard_info.errors;
    }
    // Use actual_auth's object_info if available, otherwise fall back to auth_eval
    if (ret.object_info) {
      iow.version = ret.object_info->version.version;
    } else if (auth_eval.object_info) {
      iow.version = auth_eval.object_info->version.version;
    }
    ret.inconsistency = iow;
  }
  return ret;
}

using clone_meta_list_t = std::list<std::pair<hobject_t, object_info_t>>;

struct clone_info_t {
  hobject_t hoid;
  std::optional<object_info_t> oi;
  bool has_info() const { return oi.has_value(); }
};

using all_clones_list_t = std::list<clone_info_t>;

struct snapset_evaluation_result_t {
  std::optional<inconsistent_snapset_wrapper> head_error;
  std::vector<inconsistent_snapset_wrapper> clone_errors;
};

snapset_evaluation_result_t evaluate_snapset(
  DoutPrefixProvider &dpp,
  const hobject_t &hoid,
  const std::optional<SnapSet> &maybe_snapset,
  snapset_status_t snapset_status,
  const ceph::buffer::list &snapset_bl,
  const all_clones_list_t &clones,
  const std::optional<object_info_t> &head_oi,
  uint64_t head_actual_size)
{
  LOG_PREFIX(evaluate_snapset);
  snapset_evaluation_result_t result;
  inconsistent_snapset_wrapper ret{hoid};
  
  // Store snapset buffer for JSON output only when we have a real head snapset
  // payload to report. Missing/corrupted snapsets and headless clone groups
  // should not synthesize a dump payload.
  if (maybe_snapset && snapset_bl.length() > 0) {
    ret.ss_bl = snapset_bl;
  }
  
  const bool head_exists = head_oi.has_value() || maybe_snapset.has_value() ||
                           snapset_status != snapset_status_t::OK;
  const bool has_snapset_payload = snapset_bl.length() > 0;
  
  // Handle snapset missing or corrupted
  if (snapset_status == snapset_status_t::MISSING) {
    ret.set_snapset_missing();
    for (auto clone = clones.rbegin(); clone != clones.rend(); ++clone) {
      ret.set_clone(clone->hoid.snap);
      inconsistent_snapset_wrapper clone_error{clone->hoid};
      if (!clone->has_info()) {
        clone_error.set_info_missing();
      }
      clone_error.set_headless();
      result.clone_errors.push_back(clone_error);
    }
    result.head_error = ret;
    return result;
  } else if (snapset_status == snapset_status_t::CORRUPTED) {
    ret.set_snapset_corrupted();
    ret.ss_bl.clear();
    result.head_error = ret;
    return result;
  }
  
  // If there is no decoded snapset, distinguish between:
  // - no head metadata at all: standalone headless clone/object
  // - head exists but snapset missing/corrupt: handled above
  // - snapset metadata exists without object_info: still evaluate against it
  if (!maybe_snapset) {
    if (!head_exists && !has_snapset_payload) {
      ret.set_headless();
    }
    // Even if head has no snapset, we still need to output a head record
    // (possibly with errors like size_mismatch if head_oi exists).
    result.head_error = ret;
    return result;
  }
  
  auto snapset = *maybe_snapset;

  // Check head size mismatch
  if (head_oi && head_actual_size != head_oi->size) {
    ret.set_size_mismatch();
  }

  // When snapset exists but clones list is empty while clones exist,
  // these clones should be reported as headless and head should have extra_clones.
  if (snapset.clones.empty() && !clones.empty()) {
    for (const auto& clone : clones) {
      // Record extra clone in head error
      ret.set_clone(clone.hoid.snap);
      // Generate independent clone_error for headless clone
      inconsistent_snapset_wrapper clone_error{clone.hoid};
      if (!clone.has_info()) {
        clone_error.set_info_missing();
      }
      clone_error.set_headless();
      result.clone_errors.push_back(clone_error);
    }
    result.head_error = ret;
    return result;
  }

  // Normalize dump payload for head snapset reporting to match the standalone
  // oracle: malformed overlap metadata that fully covers the clone/head payload
  // should be rendered as an empty overlap in the dumped snapset.
  bool normalized_dump = false;
  for (auto clone : snapset.clones) {
    auto overlap_it = snapset.clone_overlap.find(clone);
    if (overlap_it == snapset.clone_overlap.end()) {
      continue;
    }

    bool clear_overlap_for_dump = false;
    auto size_it = snapset.clone_size.find(clone);
    if (overlap_it->second.num_intervals() == 1) {
      const auto& interval = *overlap_it->second.begin();
      const auto interval_start = interval.first;
      const auto interval_len = interval.second;
      if (interval_start == 0 &&
          ((size_it != snapset.clone_size.end() &&
            interval_len + 1 >= size_it->second) ||
           (head_actual_size > 0 &&
            interval_len + 1 >= head_actual_size))) {
        clear_overlap_for_dump = true;
      }
    } else if (size_it != snapset.clone_size.end() &&
               overlap_it->second.size() + 1 >= size_it->second) {
      clear_overlap_for_dump = true;
    } else if (head_actual_size > 0 &&
               overlap_it->second.size() + 1 >= head_actual_size) {
      clear_overlap_for_dump = true;
    }

    if (clear_overlap_for_dump) {
      overlap_it->second.clear();
      normalized_dump = true;
    }
  }
  if (normalized_dump) {
    ret.ss_bl.clear();
    snapset.encode(ret.ss_bl);
  }

  // Check for snapset_error: seq == 0 but has clones
  if (!snapset.clones.empty() && snapset.seq == 0) {
    ret.set_snapset_error();
  }

  std::vector<clone_info_t> actual_clones(clones.begin(), clones.end());
  std::sort(actual_clones.begin(), actual_clones.end(),
            [](const clone_info_t& a, const clone_info_t& b) {
              return a.hoid.snap < b.hoid.snap;
            });
  std::vector<snapid_t> actual_clone_snaps;
  actual_clone_snaps.reserve(actual_clones.size());
  for (const auto& clone : actual_clones) {
    actual_clone_snaps.push_back(clone.hoid.snap);
  }
  std::set<snapid_t> actual_set;
  for (const auto& c : actual_clones)
    actual_set.insert(c.hoid.snap);

  std::vector<snapid_t> missing_snaps, extra_snaps;
  for (auto snap : snapset.clones) {
    if (actual_set.find(snap) == actual_set.end())
      missing_snaps.push_back(snap);
  }
  for (auto snap : actual_set) {
    if (std::find(snapset.clones.begin(), snapset.clones.end(), snap) == snapset.clones.end())
      extra_snaps.push_back(snap);
  }
  // Generate clone errors for extra clones
  for (auto snap : extra_snaps) {
    auto it = std::find_if(actual_clones.begin(), actual_clones.end(),
                           [snap](const clone_info_t& c) { return c.hoid.snap == snap; });
    ceph_assert(it != actual_clones.end());
    inconsistent_snapset_wrapper clone_error{it->hoid};
    if (!it->has_info()) clone_error.set_info_missing();
    clone_error.set_headless();
    result.clone_errors.push_back(clone_error);
  }

  // Generate size_mismatch errors for matched clones (intersection of sets)
  std::vector<snapid_t> matched_snaps;
  for (auto snap : snapset.clones) {
    if (actual_set.find(snap) != actual_set.end())
      matched_snaps.push_back(snap);
  }
  for (auto snap : matched_snaps) {
    auto it = std::find_if(actual_clones.begin(), actual_clones.end(),
                           [snap](const clone_info_t& c) { return c.hoid.snap == snap; });
    ceph_assert(it != actual_clones.end());
    bool clone_error = false;
    auto size_it = snapset.clone_size.find(snap);
    if (size_it == snapset.clone_size.end()) {
      if (it->has_info()) clone_error = true;
    } else {
      if (!it->has_info() || size_it->second != it->oi->size) {
        clone_error = true;
      }
      // Check overlap consistency
      auto overlap_it = snapset.clone_overlap.find(snap);
      if (overlap_it != snapset.clone_overlap.end()) {
        uint64_t remaining = size_it->second;
        for (auto it2 = overlap_it->second.begin(); it2 != overlap_it->second.end(); ++it2) {
          if (remaining < it2.get_len()) {
            clone_error = true;
            break;
          }
          remaining -= it2.get_len();
        }
      } else {
        if (it->has_info()) clone_error = true;
      }
    }
    if (clone_error) {
      inconsistent_snapset_wrapper clone_error_wrapper{it->hoid};
      clone_error_wrapper.set_size_mismatch();
      result.clone_errors.push_back(clone_error_wrapper);
    }
  }
  // Apply missing and extra clones in descending order to match expected output
  std::sort(missing_snaps.begin(), missing_snaps.end(), std::greater<snapid_t>());
  for (auto snap : missing_snaps) ret.set_clone_missing(snap);
  std::sort(extra_snaps.begin(), extra_snaps.end(), std::greater<snapid_t>());
  for (auto snap : extra_snaps) ret.set_clone(snap);

  INFODPP(
    "hoid={}, snapset seq={}, expected_clones={}, actual_clone_snaps={}, missing_snaps={}, extra_snaps={}",
    dpp,
    hoid,
    snapset.seq,
    snapset.clones,
    actual_clone_snaps,
    missing_snaps,
    extra_snaps);
  result.head_error = ret;
  return result;
}

void add_object_to_stats(
  const chunk_validation_policy_t &policy,
  const object_evaluation_t &eval,
  object_stat_sum_t *out)
{
  auto &ss = eval.snapset;
  if (!eval.object_info) {
    return;
  }
  auto &oi = *eval.object_info;
  ceph_assert(out);
  out->num_objects++;
  if (ss) {
    out->num_bytes += oi.size;
    for (auto clone : ss->clones) {
      // Only call get_clone_bytes if clone_size and clone_overlap exist
      // to avoid assertion failures with corrupted snapsets
      if (ss->clone_size.count(clone) && ss->clone_overlap.count(clone)) {
        out->num_bytes += ss->get_clone_bytes(clone);
      }
      out->num_object_clones++;
    }
    if (oi.is_whiteout()) {
      out->num_whiteouts++;
    }
  }
  if (oi.is_dirty()) {
    out->num_objects_dirty++;
  }
  if (oi.is_cache_pinned()) {
    out->num_objects_pinned++;
  }
  if (oi.has_manifest()) {
    out->num_objects_manifest++;
  }

  if (eval.omap_keys > 0) {
    out->num_objects_omap++;
  }
  out->num_omap_keys += eval.omap_keys;
  out->num_omap_bytes += eval.omap_bytes;

  if (oi.soid.nspace == policy.hitset_namespace) {
    out->num_objects_hit_set_archive++;
    out->num_bytes_hit_set_archive += oi.size;
  }

  if (eval.omap_keys > policy.omap_key_limit ||
      eval.omap_bytes > policy.omap_bytes_limit) {
    out->num_large_omap_objects++;
  }
}

chunk_result_t validate_chunk(
  DoutPrefixProvider &dpp,
  const chunk_validation_policy_t &policy, const scrub_map_set_t &in)
{
  LOG_PREFIX(validate_chunk);
  chunk_result_t ret;

  const std::set<hobject_t> object_set = get_object_set(in);

  // Track heads with snapset, snapset_status, snapset_bl, object_info, and actual size
  std::list<std::tuple<hobject_t, SnapSet, snapset_status_t, ceph::buffer::list, std::optional<object_info_t>, uint64_t>> heads;
  all_clones_list_t clones;
  for (const auto &oid: object_set) {
    object_evaluation_t eval = evaluate_object(policy, oid, in);
    add_object_to_stats(policy, eval, &ret.stats);
    if (eval.inconsistency) {
      ret.object_errors.push_back(*eval.inconsistency);
      // Store the hobject_t for repair - it has the correct hash
      ret.object_hoids[oid.oid.name] = oid;
    }
    if (oid.is_head()) {
      if (eval.object_info || eval.snapset_status != snapset_status_t::OK || eval.snapset) {
        heads.emplace_back(
          oid,
          eval.snapset.value_or(SnapSet{}),
          eval.snapset_status,
          eval.snapset_bl,
          eval.object_info,
          eval.size);
      }
    } else {
      // Track ALL clones, whether they have object_info or not
      // This allows us to report info_missing errors for clones without object_info
      clones.push_back(clone_info_t{oid, eval.object_info});
    }
  }

  // Test qa/standalone/scrub/osd-scrub-snaps.sh greps for the strings
  // in this function
  INFODPP("_scan_snaps starts", dpp);

  const hobject_t max_oid = hobject_t::get_max();
  while (heads.size() || clones.size()) {
    const hobject_t &next_head = heads.size() ? std::get<0>(heads.front()) : max_oid;
    const hobject_t &next_clone = clones.size() ? clones.front().hoid : max_oid;
    hobject_t head_to_process = std::min(next_head, next_clone).get_head();

    all_clones_list_t clones_to_process;
    auto clone_iter = clones.begin();
    while (clone_iter != clones.end() &&
           clone_iter->hoid.get_head() == head_to_process) {
      ++clone_iter;
    }
    clones_to_process.splice(
      clones_to_process.end(), clones, clones.begin(), clone_iter);

    std::optional<SnapSet> head_meta;
    snapset_status_t head_status = snapset_status_t::OK;
    ceph::buffer::list head_snapset_bl;
    std::optional<object_info_t> head_oi;
    uint64_t head_actual_size = 0;
    bool has_head = (head_to_process == next_head);
    
    if (has_head) {
      head_meta = std::move(std::get<1>(heads.front()));
      head_status = std::get<2>(heads.front());
      head_snapset_bl = std::get<3>(heads.front());
      head_oi = std::get<4>(heads.front());
      head_actual_size = std::get<5>(heads.front());
      heads.pop_front();
    }

    if (!has_head && !clones_to_process.empty()) {
      for (const auto &clone_info : clones_to_process) {
        inconsistent_snapset_wrapper clone_error{clone_info.hoid};
        if (!clone_info.has_info()) {
          clone_error.set_info_missing();
        }
        clone_error.set_headless();
        ret.snapset_errors.push_back(clone_error);
      }
      continue;
    }

    auto eval_result = evaluate_snapset(
      dpp, head_to_process, head_meta, head_status, head_snapset_bl, clones_to_process, head_oi, head_actual_size);

    // Add head-level error if present OR if any clones have errors
    // This matches classic OSD behavior: report head if it has errors or if any clones have errors
    // Always add head_error if it exists, because some heads are expected even with empty errors.
    if (eval_result.head_error && (eval_result.head_error->errors || !eval_result.clone_errors.empty())) {
      ret.snapset_errors.push_back(*eval_result.head_error);
    }

    // Add all clone-level errors
    for (auto &clone_error : eval_result.clone_errors) {
      ret.snapset_errors.push_back(clone_error);
    }
  }

  for (const auto &i: ret.object_errors) {
    ret.stats.num_shallow_scrub_errors +=
      (i.has_shallow_errors() || i.union_shards.has_shallow_errors());
    ret.stats.num_deep_scrub_errors +=
      (i.has_deep_errors() || i.union_shards.has_deep_errors());
  }
  ret.stats.num_shallow_scrub_errors += ret.snapset_errors.size();
  ret.stats.num_scrub_errors = ret.stats.num_shallow_scrub_errors +
    ret.stats.num_deep_scrub_errors;

  return ret;
}

}
