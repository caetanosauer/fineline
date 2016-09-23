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

#ifndef FINELINE_LOGREC_H
#define FINELINE_LOGREC_H

#include <memory>

#include "lrtype.h"
#include "logpage.h"

namespace fineline {

template <class NodePtr>
struct Logrec
{
    Logrec(NodePtr node) : node(node)
    {
    }

    virtual void redo() = 0;
    virtual void print(std::ostream&) const = 0;

    friend std::ostream& operator<< (std::ostream& out, const Logrec<NodePtr>& n)
    {
        n->print(out);
        return out;
    }

    NodePtr node;
};

template <class N, class NodePtr>
struct LogrecInsert : public Logrec<NodePtr>
{
    using Key = typename N::KeyType;
    using Value = typename N::ValueType;

    LogrecInsert(NodePtr node, char* payload)
        : Logrec<NodePtr>(node)
    {
        LogEncoder<Key, Value>::decode(payload, &key, &value);
    }

    void redo()
    {
        N::insert(this->node, key, value, false);
    }

    void print(std::ostream& out) const
    {
        out << "insert k=" << key << " v=" << value;
    }

    Key key;
    Value value;
};


template <class N, class NodePtr>
std::unique_ptr<Logrec<NodePtr>>
ConstructLogRec(LRType type, NodePtr node, char* payload)
{
    Logrec<NodePtr>* res = nullptr;
    switch (type) {
        case LRType::Insert: res = new LogrecInsert<N, NodePtr>(node, payload); break;
        default:
            auto what = "I don't know how to redo this log record";
            throw std::runtime_error(what);
    }

    return std::unique_ptr<Logrec<NodePtr>>{res};
}


} // namespace fineline

#endif
