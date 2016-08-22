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

namespace fineline {

template <class Node>
struct Logrec
{
    Logrec(Node& node) : node(&node)
    {
    }

    virtual void redo() = 0;
    virtual void print(std::ostream&) const = 0;

    friend std::ostream& operator<< (std::ostream& out, const Logrec<Node>& n)
    {
        n.print(out);
        return out;
    }

    Node* node;
};

template <class Node>
struct LogrecInsert : public Logrec<Node>
{
    using Key = typename Node::KeyType;
    using Value = typename Node::ValueType;

    LogrecInsert(Node& node, char* payload)
        : Logrec<Node>(node)
    {
        LogEncoder<Key, Value>::decode(payload, &key, &value);
    }

    void redo()
    {
        this->node->insert(key, value, false);
    }

    void print(std::ostream& out) const
    {
        out << "insert k=" << key << " v=" << value;
    }

    Key key;
    Value value;
};


template <class Node>
std::unique_ptr<Logrec<Node>>
ConstructLogRec(LRType type, Node& node, char* payload)
{
    Logrec<Node>* res = nullptr;
    switch (type) {
        case LRType::Insert: res = new LogrecInsert<Node>(node, payload); break;
    }

    return std::unique_ptr<Logrec<Node>>{res};
}


} // namespace fineline

#endif
