#include <catch2/catch_test_macros.hpp>
#include <cmath>

#include "recorder.h"

class Approx {
   public:
    Approx(double value) : value(value), epsilonVal(0.001) {}

    Approx& epsilon(double new_epsilon) {
        this->epsilonVal = new_epsilon;
        return *this;
    }

    bool compare(double other) const {
        return std::fabs(value - other) < epsilonVal;
    }

    bool operator!=(double other) const {
        return !compare(other);
    }

   private:
    double value;
    double epsilonVal;
};
// Non-member operator== to allow double == Approx
inline bool operator==(double lhs, const Approx& rhs) {
    return rhs.compare(lhs);
}

// Non-member operator== to allow Approx == double
inline bool operator==(const Approx& lhs, double rhs) {
    return lhs.compare(rhs);
}
TEST_CASE("Metrics: Recording and Calculating Averages") {
    Metrics metrics;

    metrics.record("latency", 10.0);
    metrics.record("latency", 15.0);
    metrics.record("latency", 20.0);

    double avg = metrics.average("latency");
    REQUIRE(avg == Approx(15.0).epsilon(0.001));
}

TEST_CASE("Metrics: Calculating Standard Deviation") {
    Metrics metrics;

    metrics.record("latency", 10.0);
    metrics.record("latency", 20.0);
    metrics.record("latency", 30.0);

    double stddev = metrics.stddev("latency");
    REQUIRE(stddev == Approx(8.1649658093).epsilon(0.001));
}
