import pandas as pd
import numpy as np

# Sample data
data = {
    'Name': ['Alice', 'Bob', 'ALICE', 'Charlie', 'bob', None],
    'Age': [25, 30, 25, np.nan, 30, 22],
    'Gender': ['F', 'M', 'F', 'M', 'm', 'F'],
    'JoinDate': ['2022-01-01', '01/02/2022', '2022-01-01', '2022-04-01', '01-02-2022', None],
    'Salary': [50000, 60000, 50000, 700000, 60000, 30000]
}

df = pd.DataFrame(data)
print("Original Data:")
print(df)

# 1. Remove Duplicates
df = df.drop_duplicates()

# 2. Handle Missing Values
df['Age'].fillna(df['Age'].median(), inplace=True)
df['JoinDate'].fillna(method='ffill', inplace=True)
df['Name'].fillna('Unknown', inplace=True)

# 3. Fix Data Types
df['JoinDate'] = pd.to_datetime(df['JoinDate'], errors='coerce')

# 4. Handle Outliers (e.g., Salary > 3x IQR)
Q1 = df['Salary'].quantile(0.25)
Q3 = df['Salary'].quantile(0.75)
IQR = Q3 - Q1
upper_bound = Q3 + 1.5 * IQR
df['Salary'] = np.where(df['Salary'] > upper_bound, upper_bound, df['Salary'])

# 5. Standardize Text
df['Name'] = df['Name'].str.lower().str.strip()
df['Gender'] = df['Gender'].str.upper()

# 6. Correct Inconsistencies
df['Gender'] = df['Gender'].replace({'M': 'Male', 'F': 'Female'})

# Final Cleaned Data
print("\nCleaned Data:")
print(df)
