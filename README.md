# 🧹 CleanX - Automated Data Cleaning

<div align="center">
  
  ![Python](https://img.shields.io/badge/Python-3.9%2B-blue)
  ![Flask](https://img.shields.io/badge/Flask-2.3%2B-green)
  ![Pandas](https://img.shields.io/badge/Pandas-2.0%2B-yellow)
  ![License](https://img.shields.io/badge/License-MIT-red)
  
  **Upload CSV → Get Clean Data → Done.**
  
  [Features](#features) • [Quick Start](#quick-start) • [Screenshots](#screenshots) • [Tech Stack](#tech-stack)
</div>

---

## ✨ What is CleanX?

CleanX is an automated data cleaning tool that transforms messy CSV files into clean, production-ready datasets through an 8-step AI-powered pipeline. No manual cleaning needed!

**Just 3 steps:** Upload → Configure → Download

---

## 🚀 Quick Start (30 seconds)

### Prerequisites
- Python 3.9 or higher

### One-time Setup
```bash
# 1. Download CleanX
git clone https://github.com/yourusername/CleanX.git
cd CleanX

# 2. Create virtual environment
python -m venv venv

# 3. Activate it
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

# 4. Install dependencies
pip install -r requirements.txt

# 5. Create folders
mkdir data\raw data\interim data\processed logs uploads
# Mac/Linux: mkdir -p data/raw data/interim data/processed logs uploads

# 6. Run CleanX
python app.py