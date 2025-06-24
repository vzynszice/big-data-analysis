# EPA Big Data Analysis with MapReduce

A distributed computing platform for analyzing EPA (Environmental Protection Agency) air quality data using Hadoop MapReduce. This project implements five statistical algorithms optimized for big data processing and provides a user-friendly GUI for interaction with Amazon EMR clusters.

## Project Overview

This project addresses the challenge of processing millions of air quality measurements by leveraging the power of distributed computing. You can access the data here: https://www.kaggle.com/datasets/epa/epa-historical-air-quality.Instead of processing data on a single machine, we achieve significant performance improvements by distributing the workload across multiple nodes in a Hadoop cluster.

### Key Features

- **Distributed Statistical Algorithms**: Five MapReduce implementations for big data analysis
- **GUI Application**: User-friendly interface for non-technical users
- **Scalable Architecture**: Processes datasets from 1KB to 100GB+ efficiently
- **Real-time Monitoring**: Live progress tracking of MapReduce jobs
- **Performance Analytics**: Detailed timing breakdowns and processing metrics

## Implemented Algorithms

### 1. Histogram-Based Median Calculation
Traditional median calculation requires sorting all data - an O(n log n) operation. Our histogram approach reduces this to O(n) by dividing values into buckets and finding the median bucket through counting.

### 2. Min-Max Normalization (Two-Phase)
Normalizes data to [0,1] range using a two-phase MapReduce approach:
- Phase 1: Find global minimum and maximum values
- Phase 2: Apply normalization using the discovered bounds

### 3. Welford's Algorithm for Standard Deviation
Implements numerically stable, single-pass standard deviation calculation using Welford's online algorithm, perfect for distributed computing.

### 4. Skewness Calculation
Uses Terriberry's extension of Welford's method to calculate skewness (third moment) in a single pass, indicating data distribution asymmetry.

### 5. 90th Percentile Computation
Extends the histogram approach to find the value below which 90% of observations fall - crucial for air quality compliance monitoring.

## Getting Started

### Prerequisites

- Python 3.7+
- PyQt5
- Amazon EMR cluster (or local Hadoop installation)
- AWS CLI configured
- SSH key pair for EMR access

### Installation

1. Clone the repository:
git clone https://github.com/vzynszice/epa-big-data-analysis.git
cd epa-big-data-analysis

2. Install Python dependencies:
pip install PyQt5

3. Configure your environment:
cp config/config.py

### Running the GUI
python gui/main_gui.py

#### GUI Features
  - Dataset Selection: Choose from performance testing datasets or production data
  - Algorithm Selection: Pick one of five statistical functions
  - Real-time Monitoring: Watch MapReduce progress in the log window
  - Resluts Display: View formatted results with performance metrics


### Performance Results

Testing shows the overhead-dominated behavior typical of distributed systems:

| Dataset Size       | Processing Time     | Records/sec   |
|------------------|---------|----------|
| 1K Records     | 54.43 sec  | 18      |
| 5K Records     | 49.96 sec | 100 |
| 50k Records     | ~52 sec | ~960 |
| 100k Records     | ~55 sec | ~1,818 |


