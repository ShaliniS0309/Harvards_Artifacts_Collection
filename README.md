# 🏛️ Harvard’s Artifacts Collection

An end-to-end ETL and SQL analytics project using Python and Streamlit to collect, store, and interactively explore artifact data from the Harvard Art Museums API.

---

## 📌 Project Overview

Harvard’s Artifacts Collection focuses on analyzing and visualizing artifacts data to gain insights into:

* Cultural distribution of artifacts
* Periods, centuries, and classifications
* Department-wise and category-wise analysis
* Advanced query-based insights using SQL

The project is built with a strong emphasis on **clean data pipelines, query-driven analytics, and user-friendly dashboards**.

---

## 🛠️ Tech Stack

* **Python** – Data processing and application logic
* **Pandas & NumPy** – Data cleaning and transformation
* **SQL (MySQL / SQLite)** – Query-based analytics
* **SQLAlchemy** – Database connection
* **Streamlit** – Interactive web dashboard
* **GitHub** – Version control and project hosting

---

## 🔄 Project Workflow (ETL)

1. **Extract**

   * Dataset sourced from Harvard Art Museums API / CSV

2. **Transform**

   * Data cleaning and preprocessing using Pandas
   * Handling missing values and formatting columns

3. **Load**

   * Structured data stored in SQL database
   * Tables optimized for analytical queries

---

## 📊 Features

* Interactive Streamlit dashboard
* Query-based insights using SQL
* Filters using dropdowns and select boxes
* Clean UI with real-time data updates
* Error-handled database connections

---

## 🧠 Key Insights

* Distribution of artifacts by culture and classification
* Century and period-based analysis
* Department-wise artifact count
* Custom SQL queries for deeper insights

---

## 🚀 How to Run the Project

### 1️⃣ Clone the Repository

```bash
git clone https://github.com/your-username/harvards-artifacts-collection.git
cd harvards-artifacts-collection
```

### 2️⃣ Create Virtual Environment

```bash
python -m venv .venv
source .venv/bin/activate   # For Linux/Mac
.venv\Scripts\activate    # For Windows
```

### 3️⃣ Install Dependencies

```bash
pip install -r requirements.txt
```

### 4️⃣ Run Streamlit App

```bash
streamlit run art.py
```

---

## 📂 Project Structure

```
Harvards_Artifacts/
│
├── art.py                  # Streamlit application
├── data/                   # Dataset files
├── sql/                    # SQL queries
├── requirements.txt        # Project dependencies
├── README.md               # Project documentation
└── .venv/                  # Virtual environment
```

---

## 🎯 Learning Outcomes

* Hands-on experience with ETL pipelines
* Writing optimized SQL queries
* Building interactive dashboards using Streamlit
* Debugging real-world application errors
* End-to-end data analytics project exposure



## 👩‍💻 Author

**Shalini S**
Data Sceience Learner | Python | SQL | Streamlit

---

⭐ If you find this project useful, feel free to star the repository!
