// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/mman.h>
#include <string.h>

#include <memory>
#include <string.h>

#include "include/buffer.h"
#include "include/byteorder.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/extentmap_manager/btree/extentmap_btree_node.h"
#include "crimson/os/seastore/extentmap_manager/btree/extentmap_btree_node_impl.h"
namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace crimson::os::seastore::extentmap_manager {

std::ostream &ExtMapInnerNode::print_detail(std::ostream &out) const
{
  return out << ", size=" << get_size()
	     << ", depth=" << depth;
}
ExtMapInnerNode::seek_lextent_ret
ExtMapInnerNode::seek_lextent(Transaction &t, uint32_t lo, uint32_t len)
{
  auto [begin, end] = bound(lo, lo + len);
  auto result_up = std::make_unique<extent_map_list_t>();
  auto &result = *result_up;
  return crimson::do_for_each(
    std::move(begin),
    std::move(end),
    [this, &t, &result, lo, len](const auto &val) mutable {
      return extmap_load_extent(tm, t, val.get_val(), depth - 1).safe_then(
	  [&t, &result, lo, len](auto extent) mutable {
	    // TODO: add backrefs to ensure cache residence of parents
	    return extent->seek_lextent(t, lo, len).safe_then(
		[&t, &result, lo, len](auto item_list) mutable {
		  result.splice(result.end(), item_list,
				item_list.begin(), item_list.end());
            });
      });
  }).safe_then([result=std::move(result_up)] {
    return seek_lextent_ret( 
           seek_lextent_ertr::ready_future_marker{},
           std::move(*result));
  });
}

ExtMapInnerNode::insert_ret
ExtMapInnerNode::insert(Transaction &t, uint32_t lo, lext_map_val_t val)
{
  auto insertion_pt = get_containing_child(lo);
  return extmap_load_extent(tm, t, insertion_pt->get_val(), depth-1).safe_then(
    [this, insertion_pt, &t, lo, val=std::move(val)](auto extent) mutable {
      return extent->at_max_capacity() ?
        split_entry(t, lo, insertion_pt, extent) :
        insert_ertr::make_ready_future<ExtMapNodeRef>(std::move(extent));
    }).safe_then([this, &t, lo, val=std::move(val)](ExtMapNodeRef extent) mutable {
      if (extent->depth == 0) {
        auto mut_extent = make_duplicate(t, extent);
        extent = mut_extent->cast<ExtMapNode>();
      }
      return extent->insert(t, lo, val);
    });
}

ExtMapInnerNode::punch_lextent_ret
ExtMapInnerNode::punch_lextent(Transaction &t, uint32_t lo, uint32_t len)
{
  auto result_up = std::make_unique<extent_map_list_t>();
  auto &result = *result_up;
  auto punch_pt = get_containing_child(lo);
  return extmap_load_extent(tm, t, punch_pt->get_val(), depth-1).safe_then(
    [this, &result, punch_pt, &t, lo, len](auto extent) mutable {
      return extent->at_max_capacity() ?
        split_entry(t, lo, punch_pt, extent) :
        punch_lextent_ertr::make_ready_future<ExtMapNodeRef>(std::move(extent));
  }).safe_then([this, &result, &t, lo, len](ExtMapNodeRef extent) mutable {
    if (extent->depth == 0) {
      auto mut_extent = make_duplicate(t, extent);
      extent = mut_extent->cast<ExtMapNode>();
    }
    return extent->punch_lextent(t, lo, len).safe_then(
      [&result](auto item_list) mutable {
        result.splice(result.end(), item_list, item_list.begin(), item_list.end());
    });
  }).safe_then([result=std::move(result_up)] {
    return punch_lextent_ret(
           punch_lextent_ertr::ready_future_marker{},
           std::move(*result));
  });

}

ExtMapInnerNode::rm_lextent_ret
ExtMapInnerNode::rm_lextent(Transaction &t, uint32_t lo, lext_map_val_t val)
{
  auto rm_pt = get_containing_child(lo);
  return extmap_load_extent(tm, t, rm_pt->get_val(), depth-1).safe_then(
    [this, rm_pt, &t, lo, val=std::move(val)](auto extent) mutable {
      return extent->rm_lextent(t, lo, val);
  });
}

ExtMapInnerNode::mutate_mapping_ret
ExtMapInnerNode::mutate_mapping(Transaction &t, uint32_t lo,
                                mutate_func_t &&f)
{
  return extmap_load_extent(tm, t, get_containing_child(lo)->get_val(), depth-1)
    .safe_then([this, &t, lo](ExtMapNodeRef extent) {
    if (extent->at_min_capacity()) {
      auto mut_this = make_duplicate(t, this)->cast<ExtMapInnerNode>();
      return mut_this->merge_entry(t, lo, mut_this->get_containing_child(lo), extent);
    } else {
      return merge_entry_ertr::make_ready_future<ExtMapNodeRef>(std::move(extent));
    }
  }).safe_then([this, &t, lo, f=std::move(f)](ExtMapNodeRef extent) mutable {
    if (extent->depth == 0) {
      auto mut_extent = make_duplicate(t, this)->cast<ExtMapNode>();
      return mut_extent->mutate_mapping(t, lo, std::move(f));
    } else {
      return extent->mutate_mapping(t, lo, std::move(f));
    }
  });
}

ExtMapInnerNode::find_hole_ret
ExtMapInnerNode::find_hole(Transaction &t, uint32_t lo, uint32_t len)
{
  assert(len < PAGE_SIZE);
  logger().debug("ExtMapInnerNode::find_hole for logic_offset={}, len={}, *this={}",
    lo, len, *this);
  auto bounds = bound(lo, lo + len);
  return seastar::do_with(
    bounds.first,
    bounds.second,
    lext_map_val_t{L_ADDR_NULL, 0, 0},   
    [this, &t, lo, len](auto &i, auto &e, auto &ret) {
      return crimson::do_until(
	[this, &t, &i, &e, &ret, lo, len] {
	  if (i == e) {
	    return find_hole_ertr::make_ready_future<ExtentRef>(
			    std::make_unique<Extent>(lo, L_ADDR_NULL, 0, 0));
	  }
	  return extmap_load_extent(tm, t, i->get_val(), depth-1)
	    .safe_then([this, &t, &i, lo, len](auto extent) mutable {
	    logger().debug(
	      "ExtMapInnerNode::find_hole in {} for lo {} len {} ",
	      *extent, lo, len);
	    return extent->find_hole(t, lo, len);
	  }).safe_then([lo, &i, &ret](auto ext_map) mutable {
	    i++;
	    if (ext_map->laddr != L_ADDR_NULL) {
	      ret.laddr = ext_map->laddr;
	      ret.lextent_offset = ext_map->lextent_offset;
	      ret.length = ext_map->length;
	    }
	    return find_hole_ertr::make_ready_future<ExtentRef>(
	           std::make_unique<Extent>(lo, ret.laddr, 
				   ret.lextent_offset, ret.length));
	  });
	}).safe_then([lo, &ret]() {
	  return find_hole_ertr::make_ready_future<ExtentRef>(
                 std::make_unique<Extent>(lo, ret.laddr,
			          ret.lextent_offset, ret.length));
	});
    });
}

ExtMapInnerNode::split_children_ret 
ExtMapInnerNode::make_split_children(Transaction &t) 
{
  return extmap_alloc_extent<ExtMapInnerNode>(t, EXTMAP_BLOCK_SIZE)
    .safe_then([this, &t] (auto &&left) {
      return extmap_alloc_extent<ExtMapInnerNode>(t,  EXTMAP_BLOCK_SIZE)
        .safe_then([this, left = std::move(left)] (auto &&right) {
          return split_children_ret(
            split_children_ertr::ready_future_marker{},
            std::make_tuple(left, right, split_into(*left, *right)));
      });
  });
}

ExtMapInnerNode::full_merge_ret
ExtMapInnerNode::make_full_merge(Transaction &t, ExtMapNodeRef &right) 
{
  return extmap_alloc_extent<ExtMapInnerNode>(t, EXTMAP_BLOCK_SIZE)
    .safe_then([this, &right] (auto &&replacement) {
      replacement->merge_from(*this, *right->cast<ExtMapInnerNode>());
      return full_merge_ret(
        full_merge_ertr::ready_future_marker{},
        std::move(replacement));
  });
}

ExtMapInnerNode::make_balanced_ret
ExtMapInnerNode::make_balanced(Transaction &t,
	       	ExtMapNodeRef &_right, bool prefer_left) 
{
  ceph_assert(_right->get_type() == type);
  auto &right = *_right->cast<ExtMapInnerNode>();
  return extmap_alloc_extent<ExtMapInnerNode>(t,  EXTMAP_BLOCK_SIZE)
    .safe_then([this, &t, &right, prefer_left] (auto && replacement_left){
      return extmap_alloc_extent<ExtMapInnerNode>(t, EXTMAP_BLOCK_SIZE)
        .safe_then([this, &right, prefer_left, 
	replacement_left = std::move(replacement_left)]
                   (auto &&replacement_right) {
          return make_balanced_ret(
            make_balanced_ertr::ready_future_marker{},
            std::make_tuple(replacement_left, replacement_right,
            balance_into_new_nodes(*this, right, prefer_left,
                                   *replacement_left, *replacement_right)));
      });
  });
}
ExtMapInnerNode::split_entry_ret
ExtMapInnerNode::split_entry(Transaction &t, uint32_t lo,
  internal_iterator_t iter, ExtMapNodeRef entry)
{
  ceph_assert(!at_max_capacity());
  return entry->make_split_children(t)
    .safe_then([this, &t, lo, iter, entry] (auto tuple){ 
    auto [left, right, pivot] = tuple;
    journal_update(iter, left->get_laddr(), maybe_get_delta_buffer());
    journal_insert(iter + 1, pivot, right->get_laddr(), maybe_get_delta_buffer());
    //retire extent
    return tm->dec_ref(t, entry->get_laddr())
      .safe_then([this, lo, left = std::move(left), right = std::move(right), pivot] 
      (auto ret) {
      logger().debug(
        "ExtMapInnerNode::split_entry *this {} left {} right {}",
        *this, *left, *right);

      return split_entry_ertr::make_ready_future<ExtMapNodeRef>(
             pivot > lo ? left : right);
    });
  });
}

ExtMapInnerNode::merge_entry_ret
ExtMapInnerNode::merge_entry(Transaction &t, uint32_t lo,
  internal_iterator_t iter, ExtMapNodeRef entry)
{
  logger().debug("ExtMapInnerNode: merge_entry: {}, {}", *this, *entry);
  auto is_left = (iter + 1) == end();
  auto donor_iter = is_left ? iter - 1 : iter + 1;
  return extmap_load_extent(tm, t, donor_iter->get_val(), depth-1)
    .safe_then([this, &t, lo, iter, entry, donor_iter, is_left]
      (auto donor) mutable {
    auto [l, r] = is_left ?
      std::make_pair(donor, entry) : std::make_pair(entry, donor);
    auto [liter, riter] = is_left ?
      std::make_pair(donor_iter, iter) : std::make_pair(iter, donor_iter);
    if (donor->at_min_capacity()) {
      return l->make_full_merge(t, r)
        .safe_then([this, &t, lo, iter, entry, donor_iter, is_left, l, r, liter, riter]
	(auto replacement){
        journal_update(liter, replacement->get_laddr(), maybe_get_delta_buffer());
        journal_remove(riter, maybe_get_delta_buffer());

        //retire extent
        return tm->dec_ref(t, l->get_laddr())
          .safe_then([this, r, replacement = std::move(replacement), &t] (auto ret) {
	  return tm->dec_ref(t, r->get_laddr())
            .safe_then([replacement = std::move(replacement)] (auto ret) {     
            return merge_entry_ertr::make_ready_future<ExtMapNodeRef>
	                                       (std::move(replacement));
          });
	});
      });
    } else {
      logger().debug("ExtentMapInternalEntry::merge_entry balanced l {} r {}",
	*l, *r);
      return l->make_balanced(t, r, !is_left)
	.safe_then([this, &t, lo, iter, entry, donor_iter, is_left, l, r, liter, riter]
	(auto tuple) {
	auto [replacement_l, replacement_r, pivot] = tuple;
        journal_update(liter, replacement_l->get_laddr(), maybe_get_delta_buffer());
        journal_replace(riter, pivot, replacement_r->get_laddr(),
   	                maybe_get_delta_buffer());

      
        // retire extent? 
        return tm->dec_ref(t, l->get_laddr())
          .safe_then([this, &t, lo, r, pivot, replacement_l = std::move(replacement_l), 
			  replacement_r = std::move(replacement_r)] (auto ret) {
          return tm->dec_ref(t, r->get_laddr())
            .safe_then([lo, pivot, replacement_l = std::move(replacement_l),
			   replacement_r = std::move(replacement_r)] (auto ret) {
            return merge_entry_ertr::make_ready_future<ExtMapNodeRef>(
               lo >= pivot ? replacement_r : replacement_l);
          });
        });
      });	  
    }
  });
}


ExtMapInnerNode::internal_iterator_t
ExtMapInnerNode::get_containing_child(uint32_t lo)
{
  // TODO: binary search
  for (auto i = begin(); i != end(); ++i) {
    if (i.contains(lo))
      return i;
  }
  ceph_assert(0 == "invalid");
  return end();
}

std::ostream &ExtMapLeafNode::print_detail(std::ostream &out) const
{
  return out << ", size=" << get_size()
	     << ", depth=" << depth;
}

ExtMapLeafNode::seek_lextent_ret
ExtMapLeafNode::seek_lextent(Transaction &t, uint32_t lo, uint32_t len)
{
  logger().debug(
    "ExtMapLeafNode::seek_lextent {}~{}", lo, len);
  auto ret = extent_map_list_t();
  auto [i, end] = get_leaf_entries(lo, len);
  for (; i != end; ++i) {
    auto val = (*i).get_val();
    ret.emplace_back(
      std::make_unique<Extent>(
	(*i).get_key(),
	val.laddr,
	val.lextent_offset,
	val.length));
  }
  return seek_lextent_ertr::make_ready_future<extent_map_list_t>(
    std::move(ret));
}

ExtMapLeafNode::insert_ret
ExtMapLeafNode::insert(Transaction &t, uint32_t lo, lext_map_val_t val)
{
  ceph_assert(!at_max_capacity());
  auto insert_pt = lower_bound(lo);
  journal_insert(insert_pt, lo, val, maybe_get_delta_buffer());

  logger().debug(
    "ExtMapLeafNode::insert: inserted {}~{} -> {}",
    insert_pt.get_key(),
    insert_pt.get_val().laddr,
    insert_pt.get_val().lextent_offset,
    insert_pt.get_val().length);
  return insert_ertr::make_ready_future<ExtentRef>(
    std::make_unique<Extent>(lo, val.laddr, val.lextent_offset, val.length));
}

ExtMapLeafNode::punch_lextent_ret
ExtMapLeafNode::punch_lextent(Transaction &t, uint32_t lo, uint32_t len)
{
  auto ret = extent_map_list_t();
  auto deflist = std::list<laddr_t>();
  auto [punch_pt, punch_end] = get_leaf_entries(lo, len);
  uint32_t loend = lo + len; 
  for (; punch_pt != punch_end; ++punch_pt) {
    if (punch_pt->get_key() >= loend)
      break;
    
    auto val = (*punch_pt).get_val();
    if ( punch_pt->get_key() < lo) {
      if (punch_pt->get_key() + val.length > loend) {
        //split and deref middle
	uint32_t front = lo - punch_pt->get_key();
	ret.emplace_back(
	  std::make_unique<Extent>(
            lo, 
	    val.laddr,
	    val.lextent_offset + front,
	    len));
	lext_map_val_t upval(val.laddr, val.lextent_offset, front);
        journal_update(punch_pt, upval, maybe_get_delta_buffer());
        lext_map_val_t inval(val.laddr, val.lextent_offset + front + len, 
	                     val.length - front -len); 
        journal_insert(punch_pt + 1, loend, inval, maybe_get_delta_buffer());
        break;	
      } else {
        // deref tail
	ceph_assert(punch_pt->get_key() + val.length > lo);
	uint32_t keep = lo - punch_pt->get_key();
	ret.emplace_back(
          std::make_unique<Extent>(
            lo,
            val.laddr,
            val.lextent_offset + keep,
            val.length - keep));
        lext_map_val_t upval(val.laddr, val.lextent_offset, keep);
	journal_update(punch_pt, upval, maybe_get_delta_buffer());
      }
    }
    if (punch_pt->get_key() + punch_pt->get_val().length <= loend){
      //deref whole lextent
      ret.emplace_back(
        std::make_unique<Extent>(
          punch_pt->get_key(),
          val.laddr,
          val.lextent_offset,
          val.length));
      journal_remove(punch_pt, maybe_get_delta_buffer());
      deflist.push_back(val.laddr);
    }
    // deref head
    uint32_t keep = punch_pt->get_key() + val.length - loend;
    ret.emplace_back(
      std::make_unique<Extent>(
        punch_pt->get_key(),
        val.laddr,
        val.lextent_offset,
        val.length - keep));
    lext_map_val_t inval(val.laddr, val.lextent_offset + val.length -keep, keep);
    journal_insert(punch_pt + 1, loend, inval, maybe_get_delta_buffer());  
    journal_remove(punch_pt, maybe_get_delta_buffer());
    break;
  }
  return crimson::do_for_each(deflist.begin(), deflist.end(), [this, &t, &ret] 
	 (const auto &laddr) {
    return tm->dec_ref(t, laddr).safe_then([](auto i) { 
      return punch_lextent_ertr::now();
    });      
  }).safe_then ([this, &ret] {
    return punch_lextent_ertr::make_ready_future<extent_map_list_t>(std::move(ret));
  });
}

ExtMapLeafNode::rm_lextent_ret
ExtMapLeafNode::rm_lextent(Transaction &t, uint32_t lo, lext_map_val_t val)
{
  auto [rm_pt, rm_end] = get_leaf_entries(lo, val.length);
  assert(rm_pt == rm_end);
  if(lo == rm_pt->get_key() && val.laddr == rm_pt->get_val().laddr && val.length == rm_pt->get_val().length){
    journal_remove(rm_pt, maybe_get_delta_buffer());

    logger().debug(
      "ExtMapLeafNode::insert: inserted {}~{} -> {}",
      rm_pt.get_key(),
      rm_pt.get_val().laddr,
      rm_pt.get_val().lextent_offset,
      rm_pt.get_val().length);
    return rm_lextent_ertr::make_ready_future<bool>(true);
  } else {
    return rm_lextent_ertr::make_ready_future<bool>(false);
  }
}

ExtMapLeafNode::mutate_mapping_ret
ExtMapLeafNode::mutate_mapping(Transaction &t, uint32_t lo,
                               mutate_func_t &&f)
{
  ceph_assert(!at_min_capacity());
  auto mutation_pt = find(lo);
  if (mutation_pt == end()) {
    ceph_assert(0 == "should be impossible");
    return mutate_mapping_ret(
      mutate_mapping_ertr::ready_future_marker{},
      std::nullopt);
  }

  auto mutated = f(mutation_pt.get_val());
  if (mutated) {
    journal_update(mutation_pt, *mutated, maybe_get_delta_buffer());
    return mutate_mapping_ret(
      mutate_mapping_ertr::ready_future_marker{},
      mutated);
  } else {
    journal_remove(mutation_pt, maybe_get_delta_buffer());
    return mutate_mapping_ret(
      mutate_mapping_ertr::ready_future_marker{},
      mutated);
  }
}


ExtMapLeafNode::find_hole_ret
ExtMapLeafNode::find_hole(Transaction &t, uint32_t lo, uint32_t len)
{
  assert(len < PAGE_SIZE);
  logger().debug("ExtMapLeafNode::find_hole lo={}, len={}, *this={}",
                 lo, len, *this);
  auto [begin, end] = bound(lo, lo + len);
  if (begin++ != end)
    return find_hole_ertr::make_ready_future<ExtentRef>(
		    std::make_unique<Extent>(lo, L_ADDR_NULL, 0, 0));
  
  if ((lo > begin->get_key() && (begin->get_key() + begin->get_val().length) > lo)|| 
		  (lo < end->get_key() && (lo + len) > end->get_key()))
    return find_hole_ertr::make_ready_future<ExtentRef>(
                    std::make_unique<Extent>(lo, L_ADDR_NULL, 0, 0));
  laddr_t laddr;
  uint32_t lextent_offset;

  if (begin->get_val().laddr == end->get_val().laddr){
    lextent_offset = lo - begin->get_key() + begin->get_val().lextent_offset;
    laddr = begin->get_val().laddr;
  }

  if (end->get_val().lextent_offset > len){
    lextent_offset = end->get_val().lextent_offset - (end->get_key() - lo);
    laddr = end->get_val().laddr;
  } else if (((begin->get_key() + begin->get_val().length + PAGE_SIZE - 1) 
		  & ~(PAGE_SIZE - 1)) > (lo + len)){
    lextent_offset = lo - begin->get_key() + begin->get_val().lextent_offset;
    laddr = begin->get_val().laddr;
  }
  return insert(t, lo, {laddr, lextent_offset, len}).safe_then([](auto extref) {
    return find_hole_ertr::make_ready_future<ExtentRef>(std::move(extref));
  });
  return find_hole_ertr::make_ready_future<ExtentRef>(
		  std::make_unique<Extent>(lo, L_ADDR_NULL, 0, 0)); 
}

ExtMapLeafNode::split_children_ret 
ExtMapLeafNode::make_split_children(Transaction &t) 
{
  return extmap_alloc_extent<ExtMapLeafNode>(t, EXTMAP_BLOCK_SIZE)
    .safe_then([this, &t] (auto &&left) {
      return extmap_alloc_extent<ExtMapLeafNode>(t, EXTMAP_BLOCK_SIZE)
        .safe_then([this, &t, left = std::move(left)] (auto &&right){
           return split_children_ret(
             split_children_ertr::ready_future_marker{},
	     std::make_tuple(left, right, split_into(*left, *right)));
      });
  });
}
ExtMapLeafNode::full_merge_ret
ExtMapLeafNode::make_full_merge(Transaction &t, ExtMapNodeRef &right) 
{
  return extmap_alloc_extent<ExtMapLeafNode>(t, EXTMAP_BLOCK_SIZE)
    .safe_then([this, &t, &right] (auto &&replacement) {
      replacement->merge_from(*this, *right->cast<ExtMapLeafNode>());
      return full_merge_ret(
	full_merge_ertr::ready_future_marker{},
	std::move(replacement));
  });
}
ExtMapLeafNode::make_balanced_ret 
ExtMapLeafNode::make_balanced(Transaction &t,
                              ExtMapNodeRef &_right, bool prefer_left) 
{
  ceph_assert(_right->get_type() == type);
  auto &right = *_right->cast<ExtMapLeafNode>();
  return extmap_alloc_extent<ExtMapLeafNode>(t, EXTMAP_BLOCK_SIZE)
    .safe_then([this, &t, &right, prefer_left] (auto &&replacement_left) {
      return extmap_alloc_extent<ExtMapLeafNode>(t, EXTMAP_BLOCK_SIZE)
        .safe_then([this, &right, prefer_left,
          replacement_left = std::move(replacement_left)] (auto &&replacement_right){
          return make_balanced_ret(
                 make_balanced_ertr::ready_future_marker{},
                 std::make_tuple(replacement_left, replacement_right,
                     balance_into_new_nodes(*this, right, prefer_left,
                       *replacement_left, *replacement_right)));
    });
  });
}


std::pair<ExtMapLeafNode::internal_iterator_t, ExtMapLeafNode::internal_iterator_t>
ExtMapLeafNode::get_leaf_entries(uint32_t addr, uint32_t len)
{
  return bound(addr, addr + len);
}


TransactionManager::read_extent_ertr::future<ExtMapNodeRef>
extmap_load_extent(TransactionManager* tm, Transaction& txn, laddr_t laddr, depth_t depth)
{
  if (depth > 0) {
    return tm->read_extents<ExtMapInnerNode>(txn, laddr, EXTMAP_BLOCK_SIZE).safe_then(
      [depth](auto&& extents) {
      assert(extents.size() == 1);
      [[maybe_unused]] auto [laddr, e] = extents.front();
      e->set_depth(depth);
      return TransactionManager::read_extent_ertr::make_ready_future<ExtMapNodeRef>(std::move(e));
    });
  } else {
    return tm->read_extents<ExtMapLeafNode>(txn, laddr, EXTMAP_BLOCK_SIZE).safe_then(
      [depth](auto&& extents) {
      assert(extents.size() == 1);
      [[maybe_unused]] auto [laddr, e] = extents.front();
      e->set_depth(depth);
      return TransactionManager::read_extent_ertr::make_ready_future<ExtMapNodeRef>(std::move(e));
    });
  }
}


}

