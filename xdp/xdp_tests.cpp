#include <catch2/catch_test_macros.hpp>
#include <iostream>

#include "directory.h"
#include "xdp.h"

TEST_CASE("Simple XDP Test") {
#ifdef ENABLE_XDP
    SECTION("Initialization") {
        XDP<TraitsGI, TraitsLI, TraitsLIBuffer> xdp(4);
        xdp.performWriteTask(1, 1);
        xdp.performWriteTask(2, 2);
        xdp.performWriteTask(3, 3);
        xdp.performWriteTask(4, 4);
        xdp.performWriteTask(5, 5);
        xdp.performWriteTask(6, 6);
        xdp.performWriteTask(7, 7);
        xdp.performWriteTask(8, 8);
        xdp.performWriteTask(9, 9);
        xdp.performWriteTask(10, 10);

        auto e = xdp.performReadTask(1);
        REQUIRE(e.has_value());
        REQUIRE(e->key == 1);
        REQUIRE(e->value == 1);

        auto res = xdp.get_memory_footprint();
        REQUIRE(res[0] == 22544);
        REQUIRE(res[1] == 53248);
        REQUIRE(res[2] == 166144);
    }

    SECTION("loop over and create 8000 data points") {
        XDP<TraitsGI, TraitsLI, TraitsLIBuffer> xdp(4000);
        for (int i = 0; i < 80010; ++i) {
            xdp.performWriteTask(i, i);
        }
        for (int i = 0; i < 80010; ++i) {
            auto e = xdp.performReadTask(i);
            REQUIRE(e.has_value());
            REQUIRE(e->key == i);
            REQUIRE(e->value == i);
        }
        // xdp.printStatus();
        auto res = xdp.get_memory_footprint();
        REQUIRE(res[1] == 532480);
        REQUIRE(res[2] == 167424);
    }
    SECTION("loop over and create data points with LI") {
        XDP<TraitsGI, TraitsLI, TraitsLIBuffer> xdp(70000);
        for (int i = 0; i < 80010; ++i) {
            xdp.performWriteTask(i, i);
        }
        for (int i = 0; i < 80010; ++i) {
            auto e = xdp.performReadTask(i);
            REQUIRE(e.has_value());
            REQUIRE(e->key == i);
            REQUIRE(e->value == i);
        }
        // xdp.printStatus();
        auto res = xdp.get_memory_footprint();
    }
#endif
}
//TEST_CASE("extra XDP Test") {
//#ifdef ENABLE_XDP
//    SECTION("Test XDP") {
//        int LI_INDEX_SIZE = 5000;
//        int total_entries = LI_INDEX_SIZE * 2 + LI_INDEX_SIZE / 2;
//        XDP<TraitsGI, TraitsLI, TraitsLIBuffer> xdp(LI_INDEX_SIZE);
//        for (int i = 0; i < total_entries; ++i) {
//            xdp.performWriteTask(i, i);
//            if (i % 10000 == 0) {
//                std::cout << "XDP memory footprint: " << xdp.get_memory_footprint_per_entry(i) << std::endl;
//                std::cout << "counter: " << i << std::endl;
//            }
//        }
//
//        for (int i = 0; i < total_entries; ++i) {
//            if (i == 525000) {
//                std::cout << "XDP memory footprint: " << xdp.get_memory_footprint_per_entry(i) << std::endl;
//                std::cout << "counter: " << i << std::endl;
//            }
//            auto e = xdp.performReadTask(i);
//            REQUIRE(e.has_value());
//            REQUIRE(e->key == i);
//            REQUIRE(e->value == i);
//        }
//    }
//#endif
//}
