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

#ifndef FINELINE_PLOG_H
#define FINELINE_PLOG_H

#include <list>
#include <memory>

#include "assertions.h"

namespace fineline {

/*
 * TODO: At this point, I'm wondering if it wouldn't be easier to allocate larger pages when an
 * overflow happens, similar to std::vector. Maintaining a list of overflowing pages makes quite
 * a few things more cumbersome. For example, at commit time, we need to iterate over pages
 * instead of simply copying a single page into the insert log buffer. On the other hand, the
 * templates currently require a fixed-size log page. Making the size dynamic (like std::vector)
 * would cause many other problems.
 */

template <class LogPage, class OverflowPlog>
class TxnPrivateLog
{
public:
    using LogPageType = LogPage;
    using Key = typename LogPage::Key;

    TxnPrivateLog()
        : page_(), has_overflown_(false)
    {
    }

    ~TxnPrivateLog()
    {
    }

    template <typename... T>
    void log(const Key& hdr, const T&... args)
    {
        if (has_overflown_) {
            overflow_.log(hdr, args...);
            return;
        }

        bool success = page_.try_insert(hdr, args...);
        if (!success) {
            start_overflow();
            log(hdr, args...);
        }
    }

    std::unique_ptr<AbstractLogIterator<Key>> iterate()
    {
        if (has_overflown_) {
            return overflow_.iterate();
        }
        return page_.iterate();
    }

    template <class LogBuffer, class RetType>
    void insert_into_buffer(LogBuffer* buffer, RetType& ret)
    {
        if (!has_overflown_) {
            ret = buffer->insert(page_);
        }
        else {
            for (auto p : overflow_.get_page_list()) {
                ret = buffer->insert(*p);
            }
        }
    }

protected:

    void start_overflow()
    {
        OverflowPlog ov;
        auto iter = page_.iterate();
        ov.import_logs(iter);

        new (&overflow_) OverflowPlog;
        overflow_ = ov;
        has_overflown_ = true;
    }

    union {
        LogPage page_;
        OverflowPlog overflow_;
    };
    bool has_overflown_;
};

template <class LogPage>
class ChainedPagesPrivateLog
{
public:
    using Key = typename LogPage::Key;
    using ThisType = ChainedPagesPrivateLog<LogPage>;

    ChainedPagesPrivateLog()
        : curr_page_(nullptr)
    {}

    ~ChainedPagesPrivateLog()
    {}

    template <typename... T>
    void log(const Key& hdr, const T&... args)
    {
        if (!curr_page_) { add_new_page(); }

        bool success = curr_page_->try_insert(hdr, args...);
        if (!success) {
            add_new_page();
            success = curr_page_->try_insert(hdr, args...);
            foster::assert<0>(success, "Record does not fit in log page");
        }
    }

    template <class Iter>
    void import_logs(Iter& iter)
    {
        Key hdr;
        char* payload;
        while (iter->next(hdr, payload)) {
            if (!curr_page_) { add_new_page(); }
            bool success = curr_page_->try_insert_raw(hdr, payload);
            if (!success) {
                add_new_page();
                success = curr_page_->try_insert_raw(hdr, payload);
                foster::assert<0>(success, "Record does not fit in log page");
            }
        }
    }

    class Iterator : public AbstractLogIterator<Key>
    {
    public:
        Iterator(ThisType* plog)
            : plog_(plog), curr_(nullptr), ended_(true)
        {
            if (plog_->pages_.size() > 0) {
                pg_iter_ = plog_->pages_.begin();
                curr_ = std::move((*pg_iter_)->iterate());
                ended_ = false;
            }
        }

        bool next(Key& hdr, char*& payload)
        {
            if (ended_) { return false; }
            bool res = curr_->next(hdr, payload);

            if (!res) {
                pg_iter_++;
                if (pg_iter_ != plog_->pages_.end()) {
                    curr_ = std::move((*pg_iter_)->iterate());
                    res = curr_->next(hdr, payload);
                }
                else { ended_ = true; }
            }

            return res;
        }

    private:
        ThisType* plog_;
        std::unique_ptr<typename LogPage::Iterator> curr_;
        typename std::list<std::shared_ptr<LogPage>>::iterator pg_iter_;
        bool ended_;
    };

    std::unique_ptr<Iterator> iterate()
    {
        return std::unique_ptr<Iterator>{new Iterator{this}};
    }

    std::list<std::shared_ptr<LogPage>>& get_page_list()
    {
        return pages_;
    }

protected:
    void add_new_page()
    {
        curr_page_ = std::make_shared<LogPage>();
        pages_.push_back(curr_page_);
    }

    std::shared_ptr<LogPage> curr_page_;
    std::list<std::shared_ptr<LogPage>> pages_;
};

} // namespace fineline

#endif

