#include "txncontext.h"

#include "fineline.h"

namespace fineline {

// Each template instance must be defined for external linkage to work!
template<> thread_local DftTxnContext* DftTxnContext::current = nullptr;

}
