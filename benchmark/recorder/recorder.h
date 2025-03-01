#pragma once
#include <algorithm>
#include <cmath>
#include <fstream>
#include <numeric>
#include <string>
#include <unordered_map>
#include <filesystem>
#include <ctime>
#include <iomanip>
#include <vector>
#include <iostream>


class Metrics {
public:
    Metrics() {
        init();
    }

    void record(const std::string& metric_name, const double value) {
        metrics[metric_name].push_back(value);
    }

    double average(const std::string& metric_name) const {
        const auto& values = metrics.at(metric_name);
        const double sum = std::accumulate(values.begin(), values.end(), 0.0);
        return sum / static_cast<double>(values.size());
    }

    double stddev(const std::string& metric_name) const {
        const auto& values = metrics.at(metric_name);
        const double mean = average(metric_name);
        const double sq_sum = std::inner_product(values.begin(), values.end(), values.begin(), 0.0);
        return std::sqrt(sq_sum / static_cast<double>(values.size()) - mean * mean);
    }

    // Calculate the percentile (0-100) for a given metric
    double percentile(const std::string& metric_name, const double percentile_value) const {
        auto values = metrics.at(metric_name);
        if (values.empty()) {
            return 0;
        }

        std::sort(values.begin(), values.end());
        size_t index = static_cast<size_t>(std::ceil((percentile_value / 100.0) * static_cast<double>(values.size()))) - 1;
        return values[index];
    }

    // Median as the 50th percentile
    double median(const std::string& metric_name) const {
        return percentile(metric_name, 50.0);
    }

    // Export the sorted full distribution for a metric
    void exportDistribution(const std::string& metric_name, const std::string& file_name) const {
        if (std::ofstream file(file_name); file.is_open()) {
            const auto& values = metrics.at(metric_name);
            std::vector<double> sorted_values = values;
            std::sort(sorted_values.begin(), sorted_values.end());

            for (const double value : sorted_values) {
                file << value << "\n";
            }
            file.close();
        }
    }

    void printStatistics(const std::string& metric_name) const {
        std::cout << "Statistics for " << metric_name << ":\n";
        std::cout << "  Average: " << average(metric_name) << "\n";
        std::cout << "  Std Dev: " << stddev(metric_name) << "\n";
        std::cout << "  Median: " << median(metric_name) << "\n";
        std::cout << "  P90: " << percentile(metric_name, 90.0) << "\n";
        std::cout << "  P99: " << percentile(metric_name, 99.0) << "\n";
    }


    void printToFile(const std::vector<std::string>& column_names, const std::string& base_file_name, const std::string& metadata) const {
        // Generate the timestamp
        auto now = std::chrono::system_clock::now();
        auto now_c = std::chrono::system_clock::to_time_t(now);

        // std::ostringstream timestamp_stream;
        // timestamp_stream << std::put_time(std::localtime(&now_c), "%Y%m%d_%H%M%S");
        // std::string timestamp = timestamp_stream.str();

        // Create the final filename
        std::string file_name = base_file_name + metadata + ".csv";

        std::cerr << "Attempting to write to file: " << file_name << std::endl;

        // Delete the file if it exists
        if (std::filesystem::exists(file_name)) {
            std::cerr << "File exists. Deleting the existing file: " << file_name << std::endl;
            std::filesystem::remove(file_name);
        }

        // Open the file for writing
        if (std::ofstream file(file_name); file.is_open()) {
            // Write metadata and timestamp at the top
            std::cerr << "Writing metadata and timestamp..." << std::endl;
//            file << "# Metadata\n" << metadata << "\n";
//            file << "# Timestamp: " << std::put_time(std::localtime(&now_c), "%Y-%m-%d %H:%M:%S") << "\n\n";

            // Write statistics for each metric
//            std::cerr << "Writing metrics statistics..." << std::endl;
//            for (const auto& metric : metrics) {
//                const std::string& metric_name = metric.first;
//
//                file << "## Statistics for " << metric_name << ":\n";
//                if (!metric.second.empty()) {
//                    file << "  Average: " << average(metric_name) << "\n";
//                    file << "  Std Dev: " << stddev(metric_name) << "\n";
//                    file << "  Median: " << median(metric_name) << "\n";
//                    file << "  P90: " << percentile(metric_name, 90.0) << "\n";
//                    file << "  P99: " << percentile(metric_name, 99.0) << "\n\n";
//                } else {
//                    file << "  No data available.\n\n";
//                }
//            }

            // Write column names
            std::cerr << "Writing column names..." << std::endl;
            for (size_t i = 0; i < column_names.size(); ++i) {
                file << column_names[i];
                if (i < column_names.size() - 1) file << ",";
            }
            file << "\n";

            // Determine the maximum number of rows
            std::cerr << "Calculating maximum rows for columns..." << std::endl;
            size_t max_rows = 0;
            for (const auto& name : column_names) {
                if (metrics.find(name) != metrics.end()) {
                    max_rows = std::max(max_rows, metrics.at(name).size());
                } else {
                    std::cerr << "Warning: Column name '" << name << "' not found in metrics." << std::endl;
                }
            }
            std::cerr << "Maximum rows calculated: " << max_rows << std::endl;

            // Write rows of data
            std::cerr << "Writing rows of data..." << std::endl;
            for (size_t i = 0; i < max_rows; ++i) {
                for (size_t j = 0; j < column_names.size(); ++j) {
                    if (metrics.find(column_names[j]) != metrics.end()) {
                        const auto& values = metrics.at(column_names[j]);
                        if (i < values.size()) {
                            file << values[i];
                        }
                    }
                    if (j < column_names.size() - 1) file << ",";
                }
                file << "\n";
            }

            std::cerr << "File written successfully to: " << file_name << std::endl;
        } else {
            std::cerr << "Error: Could not open the file '" << file_name << "'." << std::endl;
            throw std::runtime_error("Failed to open the file '" + file_name + "'. Please check if the file path is correct, the directory exists, and you have the necessary permissions to write to the file.");
        }
    }

private:
    std::unordered_map<std::string, std::vector<double>> metrics;

    void init() {
        metrics["num_entries"] = std::vector<double>();
        metrics["ten"] = std::vector<double>();
        metrics["num_bits"] = std::vector<double>();
        metrics["insertion_time"] = std::vector<double>();
        metrics["update_time"] = std::vector<double>();
        metrics["query_time"] = std::vector<double>();
        metrics["non_exs_query_time"] = std::vector<double>();
        metrics["delete_time"] = std::vector<double>();
        metrics["FPR"] = std::vector<double>();
        metrics["memory"] = std::vector<double>();
        metrics["expansion"] = std::vector<double>();
        metrics["background_deletes"] = std::vector<double>();
        metrics["avg_run_length"] = std::vector<double>();
        metrics["avg_cluster_length"] = std::vector<double>();
        metrics["LF"] = std::vector<double>();
        metrics["tail-99"] = std::vector<double>();
        metrics["tail-99-q"] = std::vector<double>();
        metrics["tail-99-u"] = std::vector<double>();
        metrics["tail-99.9"] = std::vector<double>();
        metrics["tail-99.9-u"] = std::vector<double>();
        metrics["tail-99.9-q"] = std::vector<double>();
        metrics["tail-99.99"] = std::vector<double>();
        metrics["tail-99-non-exs"] = std::vector<double>();
    }
};
