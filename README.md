# Wikimedia Page Views Analysis using Apache Spark

## 📌 Description

This project analyzes Wikimedia page view data using Apache Spark.

Each task is implemented using:

* MapReduce paradigm (Spark transformations)
* Loop-based approach (iterative processing)

We compare both approaches in terms of execution time.

---

## 📂 Dataset

Wikimedia Pageviews dataset (Jan 1, 2016).

Each record contains:

* Project code
* Page title
* Page hits
* Page size

Place the dataset file in:

```
data/pageviews.txt
```

---

## 🗂️ Project Structure

```
wikimedia-spark/
│
├── data/
│   └── pageviews.txt
│
├── results/
│   ├── task1_stats.txt
│   ├── task2_images.txt
│   └── benchmark.txt
│
├── src/
│   ├── main.py
│   ├── parser.py
│   ├── config.py
│   ├── utils.py
│   ├── benchmark.py
│   └── tasks/
│       ├── mapreduce/
│       └── loops/
│
├── requirements.txt
├── README.md
└── .gitignore
```

---

## ⚙️ Installation

Install dependencies:

```
pip install -r requirements.txt
```

---

## ▶️ Run the Project
python -m src.main

## 📊 Output

Results are automatically saved in:

```
results/
```

Includes:

* Task outputs
* Benchmark timings

---

## ⏱️ Performance Comparison

Check:

```
results/benchmark.txt
```


