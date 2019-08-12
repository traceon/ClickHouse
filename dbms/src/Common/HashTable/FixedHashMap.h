#pragma once

#include <Common/HashTable/FixedHashTable.h>
#include <Common/HashTable/HashMap.h>


template <typename KeyType, typename MappedType, typename StateType = HashTableNoState>
struct FixedHashMapCell
{
    using Mapped = MappedType;
    using State = StateType;
    using Key = KeyType;
    using value_type = PairNoInit<KeyType, MappedType>;

    Mapped mapped;
    bool full;

    FixedHashMapCell() {}
    FixedHashMapCell(const Key &, const State &) : full(true) {}
    FixedHashMapCell(const value_type & value_, const State &) : full(true), mapped(value_.second) {}

    Mapped & getSecond() { return mapped; }
    const Mapped & getSecond() const { return mapped; }
    bool isZero(const State &) const { return !full; }
    void setZero() { full = false; }
    static constexpr bool need_zero_value_storage = false;
    void setMapped(const value_type & value) { mapped = value.getSecond(); }
};

/**
  * A specialization of IteratorCellWrapper that supports separate methods
  * for reading key and value.
  */
template <typename KeyType, typename MappedType, typename StateType>
struct IteratorCellWrapper<FixedHashMapCell<KeyType, MappedType, StateType>>
{
    using Cell = FixedHashMapCell<KeyType, MappedType, StateType>;
    using Key = typename Cell::Key;
    using Mapped = typename Cell::Mapped;
    using Value = typename Cell::value_type;

    // We use const cell pointer here for both constant and non-constant
    // wrapper, to avoid adding another template parameter. Const-correctness
    // is guaranteed by that the const iterator always returns a constant cell
    // wrapper.
    const Cell * cell;
    Key key;

    const Key & getFirst() const { return key; }

    // FIXME it should be possible to remove this method altogether and use
    // getFirst() and getSecond() instead.
    // Also remove things like FixedHashMapCell::value_type = PairNoInit<KeyType, MappedType>;
    const Value getValue() const { return {getFirst(), getSecond()}; }

    const Mapped & getSecond() const { return cell->mapped; }
    Mapped & getSecond() { return const_cast<Cell *>(cell)->mapped; }
};


template <typename Key, typename Mapped, typename Allocator = HashTableAllocator>
class FixedHashMap : public FixedHashTable<Key, FixedHashMapCell<Key, Mapped>, Allocator>
{
public:
    using Base = FixedHashTable<Key, FixedHashMapCell<Key, Mapped>, Allocator>;
    using Self = FixedHashMap;
    using key_type = Key;
    using mapped_type = Mapped;
    using Cell = typename Base::cell_type;
    using value_type = typename Cell::value_type;

    using Base::Base;

    /// Merge every cell's value of current map into the destination map via emplace.
    ///  Func should have signature void(Mapped & dst, Mapped & src, bool emplaced).
    ///  Each filled cell in current map will invoke func once. If that map doesn't
    ///  have a key equals to the given cell, a new cell gets emplaced into that map,
    ///  and func is invoked with the third argument emplaced set to true. Otherwise
    ///  emplaced is set to false.
    template <typename Func>
    void ALWAYS_INLINE mergeToViaEmplace(Self & that, Func && func)
    {
        for (auto it = this->begin(), end = this->end(); it != end; ++it)
        {
            decltype(it) res_it;
            bool inserted;
            that.emplace(it->getFirst(), res_it, inserted, it.getHash());
            func(res_it->getSecond(), it->getSecond(), inserted);
        }
    }

    /// Merge every cell's value of current map into the destination map via find.
    ///  Func should have signature void(Mapped & dst, Mapped & src, bool exist).
    ///  Each filled cell in current map will invoke func once. If that map doesn't
    ///  have a key equals to the given cell, func is invoked with the third argument
    ///  exist set to false. Otherwise exist is set to true.
    template <typename Func>
    void ALWAYS_INLINE mergeToViaFind(Self & that, Func && func)
    {
        for (auto it = this->begin(), end = this->end(); it != end; ++it)
        {
            auto res_it = that.find(it->getFirst(), it.getHash());
            if (!res_it)
                func(it->getSecond(), it->getSecond(), false);
            else
                func(res_it->getSecond(), it->getSecond(), true);
        }
    }

    template <typename Func>
    void forEachValue(Func && func)
    {
        for (auto & v : *this)
            func(v);
    }

    template <typename Func>
    void forEachMapped(Func && func)
    {
        for (auto & v : *this)
            func(v.getSecond());
    }

    mapped_type & ALWAYS_INLINE operator[](Key x)
    {
        typename Base::iterator it;
        bool inserted;
        this->emplace(x, it, inserted);
        if (inserted)
            new (&it->getSecond()) mapped_type();

        return it->getSecond();
    }
};
