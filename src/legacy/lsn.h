/*
 * MIT License
 *
 * Copyright (c) 2016 Caetano Sauer
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-MT -- Multi-threaded port of the SHORE storage manager

                       Copyright (c) 2007-2009
      Data Intensive Applications and Systems Labaratory (DIAS)
               Ecole Polytechnique Federale de Lausanne

                         All Rights Reserved.

   Permission to use, copy, modify and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies of
   the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.

   This code is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
   DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
   RESULTING FROM THE USE OF THIS SOFTWARE.
*/

/*<std-header orig-src='shore' incl-file-exclusion='SM_S_H'>

 $Id: lsn.h,v 1.5 2010/12/08 17:37:34 nhall Exp $

SHORE -- Scalable Heterogeneous Object REpository

Copyright (c) 1994-99 Computer Sciences Department, University of
                      Wisconsin -- Madison
All Rights Reserved.

Permission to use, copy, modify and distribute this software and its
documentation is hereby granted, provided that both the copyright
notice and this permission notice appear in all copies of the
software, derivative works or modified versions, and any portions
thereof, and that both notices appear in supporting documentation.

THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY
OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS
"AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND
FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.

This software was developed with support by the Advanced Research
Project Agency, ARPA order number 018 (formerly 8230), monitored by
the U.S. Army Research Laboratory under contract DAAB07-91-C-Q518.
Further funding for this work was provided by DARPA through
Rome Research Laboratory Contract No. F30602-97-2-0247.

*/

#ifndef FINELINE_LEGACY_LSN_H
#define FINELINE_LEGACY_LSN_H

#include <cstdint>
#include <string>
#include <iostream>

#include "metaprog.h"

namespace fineline {
namespace legacy {

/* FRJ: Major changes to lsn_t
 * Once the database runs long enough we will run out of
 * partition numbers (only 64k possible).
 * Fortunately, this is a log, so lsn_t don't last forever.
 * Eventually things become durable and the log partition
 * file gets reclaimed (deleted).
 * As long as the first partition is gone before
 * the last one fills, we can simply wrap and change the
 * sense of lsn_t comparisions.
 * as follows:
 *
   Suppose we use unsigned 8 bit partition numbers. If the current
   global lsn has pnum >= 128 (MSB set), any lsn we encounter with
   pnum < 128 (MSB clear) must be older:

   0        1        ...        126        127        128        129        ...         254        255


   On the other hand, if the current global lsn has pnum < 128
   (MSB clear), any lsn we encounter with pnum >= 128 (MSB set)
   must be *older* (ie, the pnum recently wrapped from 255 back to
   0):

   128        129        ...         254        255        0        1        ...        126        127

   The first situation is easy: regular comparisons provide the
   ordering we want; The second is trickier due to the wrapping of
   partition numbers. Fortunately, there's an easy way around the
   problem!  Since the MSB of the pnum is also the MSB of the
   64-bit value holding the lsn, if comparisons interpret the
   64-bit value as signed we get the proper ordering:

   -128        -127        ...        -2        -1        0        1        ...        126        127

   We assume there are enough lsn_t that less than half are in use at
   a time. So, we divide the universe of lsn's into four regions,
   marked by the most significant two bits of the file.

   00+01 - don't care
   01+10 - unsigned comparisons
   10+11 - unsigned comparisons
   11+00 - signed comparisons

   For now, though, we'll just assume overflow doesn't happen ;)
*/

/**\brief Log Sequence Number. See \ref LSNS.
 *
 * \ingroup LSNS
 * \details
 *
 * A log sequence number points to a record in the log.
 * It consists of two parts:
 * - hi(), a.k.a., file(). This is a number that matches a log partition
 *                         file, e.g., "log.<file>"
 * - lo(), a.k.a., rba(). This is byte-offset into the log partition, and is
 *                        the first byte of a log record, or is the first
 *                        byte after the last log record in the file (where
 *                        the next log record could be written).
 *
 * All state is  stored in a single 64-bit value.
 * This reading or setting is atomic on
 * 64-bit platforms (though updates still need protection).
 * \warning This is NOT atomic on 32-bit platforms.
 *
 * Because all state fits in 64 bits,
 * there is a trade-off between maximum supported log partition size
 * and number of partitions. Two reasonable choices are:
 *
 * - 16-bit partition numbers, up to 256TB per partition
 * - 32-bit partition numbers, up to 4GB per partition
 *
 *
 * CS: generalized to a pair of integers using template
 */
template <size_t BitsHigh, size_t BitsLow>
class UnsignedNumberPair
{
public:
    static constexpr size_t BitsTotal = BitsHigh + BitsLow;
    static constexpr size_t BytesHigh = BitsHigh / 8;
    static constexpr size_t BytesLow = BitsLow / 8;
    static constexpr size_t BytesTotal = BitsTotal / 8;

    static_assert(
        BitsTotal == 8 || BitsTotal == 16 || BitsTotal == 32 || BitsTotal == 64,
        "NumberPair must add up to 8, 16, 32, or 64 bits");

    using NumType = foster::meta::UnsignedInteger<BytesTotal>;
    using HighType = foster::meta::UnsignedInteger<BytesHigh>;
    using LowType = foster::meta::UnsignedInteger<BytesLow>;

    static constexpr NumType LowMask = (1ul << BitsLow) - 1;

    UnsignedNumberPair() : _data(0) { }
    UnsignedNumberPair(NumType data) : _data(data) { }

    UnsignedNumberPair(HighType hi, LowType lo)
        : _data((hi << BitsLow) | (lo & LowMask))
    { }

    NumType data() const { return _data; }
    void set(NumType data) {_data = data;}

    HighType hi() const { return _data >> BitsLow; }
    LowType lo() const { return _data & LowMask; }

    UnsignedNumberPair<BitsHigh, BitsLow>& advance(int amt = 1)
    {
        _data += amt;
        return *this;
    }

    UnsignedNumberPair<BitsHigh, BitsLow> &operator+=(long delta)
    {
        return advance(delta);
    }

    UnsignedNumberPair<BitsHigh, BitsLow> operator+(long delta) const
    {
        return this->advance(delta);
    }

    bool operator>(const UnsignedNumberPair<BitsHigh, BitsLow>& l) const
    {
        return l < *this;
    }

    bool operator<(const UnsignedNumberPair<BitsHigh, BitsLow>& l) const
    {
        return _data < l._data;
    }

    bool operator>=(const UnsignedNumberPair<BitsHigh, BitsLow>& l) const
    {
        return !(*this < l);
    }

    bool operator<=(const UnsignedNumberPair<BitsHigh, BitsLow>& l) const
    {
        return !(*this > l);
    }

    bool operator==(const UnsignedNumberPair<BitsHigh, BitsLow>& l) const
    {
        return _data == l._data;
    }

    bool operator!=(const UnsignedNumberPair<BitsHigh, BitsLow>& l) const
    {
        return !(*this == l);
    }


    std::string str() const;

    bool is_null() const { return _data == 0; }

    static const UnsignedNumberPair<BitsHigh, BitsLow> null;
    static const UnsignedNumberPair<BitsHigh, BitsLow> max;

private:
    NumType _data;
};

template <size_t H, size_t L>
inline std::ostream& operator<<(std::ostream& o, const UnsignedNumberPair<H,L>& l)
{
    return o << l.hi() << '.' << l.lo();
}

template <size_t H, size_t L>
inline std::istream& operator>>(std::istream& i, UnsignedNumberPair<H,L>& l)
{
    char c;
    typename UnsignedNumberPair<H,L>::LowType d;
    typename UnsignedNumberPair<H,L>::HighType f;
    i >> f >> c >> d;
    l = UnsignedNumberPair<H, L>{f, d};
    return i;
}

/**
 * This template instance corresponds to our Shore-MT lsn_t
 */
using lsn_t = UnsignedNumberPair<16, 48>;

} // namespace legacy
} // namespace fineline

#endif
