# Wikimedia Page Views Analysis using Apache Spark

## 📌 Description
This project analyzes Wikimedia page view data using Apache Spark.

Each task is implemented using:
1. MapReduce paradigm (Spark transformations)
2. Loop-based approach (iterative processing)

We compare both approaches in terms of execution time.

---

## 📂 Dataset
Wikimedia Pageviews dataset.

Each record contains:
- Project code
- Page title
- Page hits
- Page size

Place dataset file in:
data/pageviews.txt

## 🗂️ Project Structure
wikimedia-spark/
│
├── data/
│   └── pageviews.txt  
│
├── results/
│   ├── task1_results.txt
│   ├── task2_results.txt
│   └── benchmark.txt
│
├── src/
│   ├── main.py
│   ├── parser.py
│   ├── config.py
│   ├── utils.py
│   ├── benchmark.py
│   │
│   ├── tasks/
│       ├── mapreduce/
│       └── loops/
│
├── requirements.txt
├── README.md
└── .gitignore

## ⚙️ Installation
pip install -r requirements.txt

## ▶️ Run the Project
python -m src.main

## 📊 Output
Results are saved automatically in:
results/
Includes:
- Task outputs
- Benchmark timings

## ⏱️ Performance Comparison
results/benchmark.txt