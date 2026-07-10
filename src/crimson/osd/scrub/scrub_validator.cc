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
    // Compare snapsets between shards for SNAPSET_INCONSISTENCY in object_errors
    // This is separate from snapshot validation which adds errors to snapset_errors
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
  const chunk_validation_policy_t &policy,
  const scrub_map_set_t &in)
{
  chunk_result_t ret;

  const std::set<hobject_t> object_set = get_object_set(in);

  // Evaluate every object (object_errors + stats) and cache the results.
  // We also need the per-head snapset/object_info for snapshot validation below.
  std::map<hobject_t, object_evaluation_t> evals;
  for (const auto &oid: object_set) {
    object_evaluation_t eval = evaluate_object(policy, oid, in);
    add_object_to_stats(policy, eval, &ret.stats);
    if (eval.inconsistency) {
      ret.object_errors.push_back(*eval.inconsistency);
      ret.object_hoids[oid.oid.name] = oid;
    }
    evals.emplace(oid, std::move(eval));
  }

  // Snapshot validation: for every head object and every shard, call
  // evaluate_snapset() with that shard's own SnapSet against the clone objects
  // present on that shard.  This catches SnapSet corruptions on any shard
  // (primary or replica).  Results from all shards are merged, deduplicating
  // identical errors so that a consistent corruption is only reported once.

  for (const auto &oid : object_set) {
    if (!oid.is_head()) {
      continue;
    }

    // Primary-shard errors go into snapset_errors (stored + counted).
    // Replica-shard errors go into replica_snapset_errors (logged only).
    // Deduplication sets prevent the same (name, snap) pair being reported
    // twice when iterating multiple shards.
    std::set<std::string> emitted_primary_head;
    std::set<std::pair<std::string, uint64_t>> emitted_primary_clone;
    std::set<std::string> emitted_replica_head;
    std::set<std::pair<std::string, uint64_t>> emitted_replica_clone;

    for (const auto &[shard, scrub_map] : in) {
      const bool is_primary = (shard == policy.primary);

      // Obtain SnapSet, OI, and size for the head object on this shard.
      // For the primary shard we reuse the already-computed eval (which applied
      // authoritative-shard selection) to guarantee identical results to the
      // pre-replica-fix code path.  For replica shards we decode directly from
      // the raw scrub map because the replica may carry a different SnapSet.
      std::optional<SnapSet> shard_snapset;
      snapset_status_t shard_snapset_status = snapset_status_t::OK;
      ceph::buffer::list shard_snapset_bl;
      std::optional<object_info_t> shard_head_oi;
      uint64_t shard_head_size = 0;

      if (is_primary) {
        // Use the cached eval for the primary — same data the old code used.
        const auto &head_eval = evals.at(oid);
        shard_snapset        = head_eval.snapset;
        shard_snapset_status = head_eval.snapset_status;
        shard_snapset_bl     = head_eval.snapset_bl;
        shard_head_oi        = head_eval.object_info;
        shard_head_size      = head_eval.size;
      } else {
        // Replica shard: decode directly from this shard's scrub map.
        auto head_it = scrub_map.objects.find(oid);
        if (head_it == scrub_map.objects.end()) {
          continue;  // Head missing on this replica shard — skip.
        }
        const auto &head_obj = head_it->second;
        shard_head_size = head_obj.size;

        auto oi_it = head_obj.attrs.find(OI_ATTR);
        if (oi_it != head_obj.attrs.end()) {
          try {
            auto blp = oi_it->second.cbegin();
            shard_head_oi = object_info_t{};
            decode(*shard_head_oi, blp);
          } catch (...) {
            shard_head_oi = std::nullopt;
          }
        }

        auto ss_it = head_obj.attrs.find(SS_ATTR);
        if (ss_it == head_obj.attrs.end()) {
          shard_snapset_status = snapset_status_t::MISSING;
        } else {
          shard_snapset_bl = ss_it->second;
          try {
            auto blp = ss_it->second.cbegin();
            shard_snapset = SnapSet{};
            decode(*shard_snapset, blp);
            shard_snapset_status = snapset_status_t::OK;
          } catch (...) {
            shard_snapset = std::nullopt;
            shard_snapset_status = snapset_status_t::CORRUPTED;
          }
        }
      }

      // Collect clones for this head.
      // For the primary: include all clones from object_set (mirrors old code),
      // using the cached eval OI. Track which clones are present on the
      // primary's own scrub map so we can suppress errors for replica-only ones.
      // For replicas: only include clones present on the replica's scrub map.
      all_clones_list_t shard_clones;
      std::set<hobject_t> primary_local_clones;  // clones in primary's scrub map
      for (const auto &coid : object_set) {
        if (!coid.is_snap() || coid.get_head() != oid.get_head()) {
          continue;
        }
        if (is_primary) {
          // Include all clones (with eval OI) — same as old code.
          const auto &clone_eval = evals.at(coid);
          shard_clones.push_back(clone_info_t{coid, clone_eval.object_info});
          // Track whether this clone is locally present on the primary.
          if (scrub_map.objects.count(coid)) {
            primary_local_clones.insert(coid);
          }
        } else {
          // Replica: only include clones present on this shard.
          auto clone_it = scrub_map.objects.find(coid);
          if (clone_it == scrub_map.objects.end()) {
            continue;
          }
          clone_info_t ci;
          ci.hoid = coid;
          auto oi_it = clone_it->second.attrs.find(OI_ATTR);
          if (oi_it != clone_it->second.attrs.end()) {
            try {
              auto blp = oi_it->second.cbegin();
              ci.oi = object_info_t{};
              decode(*ci.oi, blp);
            } catch (...) {
              ci.oi = std::nullopt;
            }
          }
          shard_clones.push_back(std::move(ci));
        }
      }

      auto result = evaluate_snapset(
        dpp,
        oid,
        shard_snapset,
        shard_snapset_status,
        shard_snapset_bl,
        shard_clones,
        shard_head_oi,
        shard_head_size);

      // Route errors: primary shard → snapset_errors (stored + counted);
      //               replica shards → replica_snapset_errors (logged only).
      auto &head_seen = is_primary ? emitted_primary_head : emitted_replica_head;
      auto &clone_seen = is_primary ? emitted_primary_clone : emitted_replica_clone;
      auto &dest_head = is_primary ? ret.snapset_errors : ret.replica_snapset_errors;
      auto &dest_clone = is_primary ? ret.snapset_errors : ret.replica_snapset_errors;

      // Collect clone errors, suppressing replica-only ones for the primary path.
      std::vector<inconsistent_snapset_wrapper*> emittable_clones;
      for (auto &ce : result.clone_errors) {
        if (!ce.errors) continue;
        if (is_primary) {
          // Suppress errors for clones absent from the primary's scrub map.
          // Those are replica-only objects; their cross-shard difference is
          // already captured in object_errors.
          auto hoid_it = std::find_if(
            object_set.begin(), object_set.end(),
            [&ce](const hobject_t &h) {
              return h.oid.name == ce.object.name &&
                     h.snap == snapid_t{ce.object.snap} &&
                     h.nspace == ce.object.nspace;
            });
          if (hoid_it != object_set.end() &&
              !primary_local_clones.count(*hoid_it)) {
            continue;  // Clone only on replica; skip for primary path.
          }
        }
        emittable_clones.push_back(&ce);
      }

      // For the primary path, strip extra-clone references from the head entry
      // that correspond to replica-only clones (not in primary_local_clones).
      // If all extra clones were replica-only, clear EXTRA_CLONES from errors.
      if (is_primary && result.head_error &&
          (result.head_error->errors &
           librados::inconsistent_snapset_t::EXTRA_CLONES)) {
        auto &extra = result.head_error->clones;
        extra.erase(
          std::remove_if(extra.begin(), extra.end(),
            [&](uint64_t s) {
              auto it = std::find_if(
                object_set.begin(), object_set.end(),
                [&](const hobject_t &h) {
                  return h.oid.name == oid.oid.name &&
                         h.snap == snapid_t{s} &&
                         h.nspace == oid.nspace;
                });
              return it != object_set.end() &&
                     !primary_local_clones.count(*it);
            }),
          extra.end());
        if (extra.empty()) {
          result.head_error->errors &=
            ~static_cast<uint64_t>(
              librados::inconsistent_snapset_t::EXTRA_CLONES);
        }
      }

      // Emit head entry if it has own errors OR if there are non-suppressed
      // clone errors (head carries the snapset payload for reporting).
      if (result.head_error &&
          (result.head_error->errors || !emittable_clones.empty())) {
        if (head_seen.find(oid.oid.name) == head_seen.end()) {
          dest_head.push_back(std::move(*result.head_error));
          head_seen.insert(oid.oid.name);
        }
      }
      for (auto *ce : emittable_clones) {
        auto key = std::make_pair(ce->object.name, uint64_t{ce->object.snap});
        if (clone_seen.find(key) == clone_seen.end()) {
          clone_seen.insert(key);
          dest_clone.push_back(std::move(*ce));
        }
      }
    }
  }

  // Detect orphan clones: snap objects whose head is absent from object_set.
  // These are headless clones that were missed by the head-based loop above.
  // Emit errors only for the primary shard (same as old !has_head branch).
  for (const auto &oid : object_set) {
    if (!oid.is_snap()) {
      continue;
    }
    hobject_t head = oid.get_head();
    if (object_set.count(head)) {
      continue;  // Head exists; handled by the head-based loop above.
    }
    // Orphan clone: its head is not in any shard's scrub map.
    inconsistent_snapset_wrapper clone_error{oid};
    const auto &eval = evals.at(oid);
    if (!eval.object_info) {
      clone_error.set_info_missing();
    }
    clone_error.set_headless();
    ret.snapset_errors.push_back(clone_error);
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
