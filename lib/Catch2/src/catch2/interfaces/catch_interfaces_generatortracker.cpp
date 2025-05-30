
//              Copyright Catch2 Authors
// Distributed under the Boost Software License, Version 1.0.
//   (See accompanying file LICENSE.txt or copy at
//        https://www.boost.org/LICENSE_1_0.txt)

// SPDX-License-Identifier: BSL-1.0

#include <catch2/interfaces/catch_interfaces_generatortracker.hpp>

#include <string>

namespace Catch {
    namespace Generators {

        bool GeneratorUntypedBase::countedNext() {
            auto ret = next();
            if ( ret ) {
                m_stringReprCache.clear();
                ++m_currentElementIndex;
            }
            return ret;
        }

        StringRef GeneratorUntypedBase::currentElementAsString() const {
            if ( m_stringReprCache.empty() ) {
                m_stringReprCache = stringifyImpl();
            }
            return m_stringReprCache;
        }

    } // namespace Generators
} // namespace Catch
