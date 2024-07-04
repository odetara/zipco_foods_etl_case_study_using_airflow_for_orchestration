import pandas as pd

def run_transformation():
    data = pd.read_csv(r'zipco_transaction.csv')

    # remove duplicates
    data.drop_duplicates(inplace=True)

    # Handle Missing Values (filling missing numeric values with the mean or median)
    numeric_columns = data.select_dtypes(include=['float64', 'int64' ]).columns
    for col in numeric_columns:
        data.fillna({col: data[col].mean()}, inplace=True)

    # Handle missing values (fill missing strings/objects with "unknown")
    string_columns = data.select_dtypes(include=['object']).columns
    for col in string_columns:
        data.fillna({col: 'unknown'}, inplace=True)

    # cleaning date column assignings the right data type
    data['Date'] = pd.to_datetime(data['Date'])

    # creating fact and dimension tables

    # create the product table
    products = data[['ProductName']].drop_duplicates().reset_index(drop=True)
    products.index.name = 'ProductID'
    products = products.reset_index()

    # create customer table
    customers = data[['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber', 'CustomerEmail']].drop_duplicates().reset_index(drop=True)
    customers.index.name = 'CustomerID'
    customers = customers.reset_index()

    # create staff table
    staff = data[['Staff_Name', 'Staff_Email']].drop_duplicates().reset_index(drop=True)
    staff.index.name = 'StaffID'
    staff = staff.reset_index()

    # create transactions table
    transaction = data.merge(products, on=['ProductName'], how='left') \
                      .merge(customers, on=['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber', 'CustomerEmail'], how='left') \
                      .merge(staff, on=['Staff_Name', 'Staff_Email'], how='left')

    transaction.index.name = 'TransactionID'
    transaction = transaction.reset_index() \
                            [['Date', 'TransactionID', 'ProductID', 'Quantity', 'UnitPrice', 'StoreLocation', 'PaymentType', 'PromotionApplied', 'Weather', \
                            'Temperature', 'StaffPerformanceRating', 'CustomerFeedback', 'DeliveryTime_min', 'OrderType', 'CustomerID', 'StaffID', \
                            'DayOfWeek', 'TotalSales']]

    # save data as csv file
    data.to_csv('clean_data.csv', index=False)
    products.to_csv('products.csv', index=False)
    customers.to_csv('customers.csv', index=False)
    staff.to_csv('staff.csv', index=False)
    transaction.to_csv('transaction.csv', index=False)

    print('Data Cleaning and Transformation completed successfully')