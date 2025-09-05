import pandas as pd
# Step 1: Create DataFrame
data = {
    'Name': 3 ,
    'Age': [25, 30, 35, 40, 28],
    'City': ['New York', 'Los Angeles', 'Chicago', 'New York', 'Chicago']
}

df = pd.DataFrame(data)

# Step 2: Explore data
print("Preview:")
print(df.head())

# Step 3: Filter people over 30
older_than_30 = df[df['Age'] > 30]
print("\nPeople over 30:")
print(older_than_30)

# Step 4: Group by city and find average age
avg_age_by_city = df.groupby('City')['Age'].mean()
print("\nAverage Age by City:")
print(avg_age_by_city)

# Step 5: Add a new column
df['Senior'] = df['Age'] > 30
print("\nWith 'Senior' Column:")
print(df)

# Step 6: Save the results
df.to_csv('people.csv', index=False)