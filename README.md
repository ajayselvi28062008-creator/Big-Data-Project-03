# MiniSparkRDD 🚀

A simple Python-based implementation of **Spark-like RDD (Resilient Distributed Dataset) concepts**.

This project demonstrates how RDD transformations such as `map()`, `filter()`, and `flatMap()` can be implemented using a custom **DAG (Directed Acyclic Graph)**, optimizer, and executor.

---

## Project Overview

**MiniSparkRDD** is a lightweight educational project inspired by Apache Spark RDD architecture.

The project takes data from an Amazon product CSV dataset and processes it through a sequence of transformations.

### Processing Pipeline

```text
Amazon CSV Dataset
        ↓
      Load Data
        ↓
      Create RDD
        ↓
     FILTER
(Category = Electronics)
        ↓
     FILTER
(Rating > 4)
        ↓
       MAP
(Product Name)
        ↓
     COLLECT
        ↓
   Final Results
```

---

## Objectives

* Understand the basic concept of RDDs.
* Implement RDD transformations using Python.
* Understand lazy transformation pipelines.
* Build an execution DAG.
* Execute transformations using a custom executor.
* Demonstrate `map()`, `filter()`, and `flatMap()`.
* Process CSV data using Python.

---

## Technologies Used

* **Python 3**
* Python `csv` module
* Lambda functions
* Object-Oriented Programming
* RDD concepts
* DAG-based execution

---

## Project Structure

```text
MiniSparkRDD/
│
├── Data/
│   └── amazon.csv
│
├── main.py
│
└── src/
    ├── __init__.py
    ├── dag.py
    ├── executor.py
    ├── loader.py
    ├── node.py
    ├── optimizer.py
    ├── parser.py
    ├── rdd.py
    └── utils.py
```

---

## File Description

### `main.py`

The main program that:

* Loads the Amazon dataset.
* Creates an RDD.
* Applies filtering and mapping operations.
* Collects the final results.
* Displays the output.

---

### `loader.py`

Responsible for loading the CSV file.

It uses Python's `csv.DictReader` to convert every CSV row into a dictionary.

Example:

```python
data = load_csv("Data/amazon.csv")
```

---

### `rdd.py`

Contains the main `RDD` class.

It implements:

* `map()`
* `filter()`
* `flatMap()`
* `collect()`
* `count()`

The transformation operations are stored as nodes in the DAG.

---

### `node.py`

Defines the `Node` class.

Each node represents one operation in the execution pipeline.

Examples:

```text
FILTER
MAP
FLATMAP
```

---

### `dag.py`

Contains the `DAG` class.

The DAG stores the sequence of operations and displays the execution pipeline.

Example:

```text
Execution DAG

↓ FILTER
↓ FILTER
↓ MAP
```

---

### `executor.py`

The `Executor` class executes the operations stored in the DAG.

It supports:

```text
FILTER → Python filter()
MAP    → Python map()
FLATMAP → Custom flatMap execution
```

---

### `optimizer.py`

Contains the `Optimizer` class.

Currently, it demonstrates the optimizer stage of the architecture and returns the DAG without modifying it.

```text
RDD
 ↓
DAG
 ↓
Optimizer
 ↓
Executor
```

---

### `parser.py`

Provides helper functions for cleaning and converting values.

Functions include:

* `clean_price()`
* `clean_percentage()`
* `clean_rating()`

These functions convert text-based values into numeric values.

---

### `utils.py`

Contains helper functions for displaying the final results.

---

## Dataset

The project uses an Amazon product dataset containing information such as:

* Product ID
* Product Name
* Category
* Discounted Price
* Actual Price
* Discount Percentage
* Rating
* Rating Count
* Product Description

Example:

```text
product_id
product_name
category
discounted_price
actual_price
discount_percentage
rating
rating_count
about_product
```

---

## How It Works

The project starts by loading the Amazon CSV dataset.

```python
data = load_csv("Data/amazon.csv")
```

Then an RDD is created:

```python
amazon_rdd = RDD(data)
```

The project applies two filters:

```python
.filter(
    lambda x: "Electronics" in x["category"]
)
```

This selects products belonging to the Electronics category.

Then:

```python
.filter(
    lambda x: float(x["rating"]) > 4
)
```

This selects products with a rating greater than 4.

Finally, `map()` extracts only the product names:

```python
.map(
    lambda x: x["product_name"]
)
```

The results are obtained using:

```python
.collect()
```

---

## RDD Operations

### 1. `filter()`

Filters records based on a condition.

Example:

```python
rdd.filter(lambda x: float(x["rating"]) > 4)
```

---

### 2. `map()`

Transforms each record into another value.

Example:

```python
rdd.map(lambda x: x["product_name"])
```

---

### 3. `flatMap()`

Maps each element and combines multiple returned values into a single list.

Example:

```python
rdd.flatMap(lambda x: x["about_product"].split())
```

---

### 4. `collect()`

Executes the complete DAG and returns the final results.

```python
result = rdd.collect()
```

---

### 5. `count()`

Returns the number of records after executing the transformations.

```python
total = rdd.count()
```

---

## DAG Execution

Every transformation is represented as a node in the DAG.

For the current example:

```text
FILTER
  ↓
FILTER
  ↓
MAP
```

The executor processes these operations sequentially.

```text
Input Data
    ↓
 FILTER
    ↓
 FILTER
    ↓
   MAP
    ↓
Output
```

---

## How to Run

### Step 1: Clone the Repository

```bash
git clone <your-repository-url>
```

### Step 2: Open the Project

```bash
cd MiniSparkRDD
```

### Step 3: Check the Dataset Path

Open `main.py` and update the CSV path if necessary.

Example:

```python
data = load_csv(
    "Data/amazon.csv"
)
```

### Step 4: Run the Program

```bash
python main.py
```

---

## Expected Output

The program displays the execution DAG:

```text
Before Execution:

Execution DAG

↓ FILTER
↓ FILTER
↓ MAP
```

Then the optimizer stage:

```text
Optimizer Running...
```

Finally, the matching Amazon product names are displayed under:

```text
Final Result
```

---

## Key Concepts Demonstrated

* RDD Architecture
* Transformations
* Actions
* Lazy-style execution
* DAG construction
* DAG execution
* Basic optimization stage
* Functional programming
* CSV data processing
* Object-Oriented Programming

---

## Future Enhancements

The project can be extended with:

* Real DAG optimization
* Additional RDD transformations
* `reduce()` operation
* `groupByKey()`
* `sortBy()`
* Partition-based processing
* Parallel execution
* Caching
* Larger datasets
* Performance comparison with Apache Spark

---

## Conclusion

MiniSparkRDD provides a simple and understandable implementation of Spark-like RDD processing using Python. It demonstrates how data can flow through transformations, how operations can be represented using a DAG, and how an executor can process the DAG to produce the final result.
