import models
import pandas as pd
import datetime
import tqdm

def load_suppliers(suppliers_df: pd.DataFrame):
    new_suppliers = list()
    for i in tqdm.tqdm(range(len(suppliers_df))):
        row = suppliers_df.iloc[i]
        new_supplier = models.SupplierDim.create(
            supplier_id=row['SupplierID'],
            company_name=row['CompanyName'],
            contact_name=row['ContactName'],
            address=row['Address'],
            city=row['City'],
            region=row['Region'],
            country=row['Country'],
            postal_code=row['PostalCode'],
            phone=row['Phone'],
            fax=row['Fax']
        )
        new_suppliers.append(new_supplier)
    return new_suppliers

def load_products(products_df: pd.DataFrame):
    new_products = list()
    for i in tqdm.tqdm(range(len(products_df))):
        row = products_df.iloc[i]
        new_product = models.ProductDim.create(
            product_id=row['ProductID'],
            name=row['ProductName'],
            description=None,
            quantity_per_unit=row['QuantityPerUnit'],
            discontinued=bool(int(row['Discontinued'])))
        new_products.append(new_product)
    return new_products

def load_customers(customers_df: pd.DataFrame):
    new_customers = list()
    for i in tqdm.tqdm(range(len(customers_df))):
        row = customers_df.iloc[i]
        new_customer = models.CustomerDim.create(
            customer_id=row['CustomerID'],
            company_name=row['CompanyName'],
            contact_name=row['ContactName'],
            phone=row['Phone'],
            address=row['Address'],
            region=row['Region'],
            city=row['City'],
            country=row['Country'],
            postal_code=row['PostalCode'])
        new_customers.append(new_customer)
    return new_customers

def load_employees(employees_df: pd.DataFrame):
    new_employees = list()
    for i in tqdm.tqdm(range(len(employees_df))):
        row = employees_df.iloc[i]
        new_employee = models.EmployeeDim.create(
            employee_id=row['EmployeeID'],
            first_name=row['FirstName'],
            last_name=row['LastName'],
            region=row['Region'],
            city=row['City'],
            country=row['Country'],
            phone=row['HomePhone'],
            address=row['Address'])
        new_employees.append(new_employee)
    return new_employees

def load_categories(categories_df: pd.DataFrame):
    new_categories = list()
    for i in tqdm.tqdm(range(len(categories_df))):
        row = categories_df.iloc[i]
        new_category = models.CategoryDim.create(
            category_id=row['CategoryID'],
            name=row['CategoryName'],
            description=row['Description'])
        new_categories.append(new_category)
    return new_categories

def load_product_categories(products_df: pd.DataFrame):
    new_product_categories = list()
    for i in tqdm.tqdm(range(len(products_df))):
        row = products_df.iloc[i]
        product_id = row['ProductID']
        category_id = row['CategoryID']
        new_product_category = models.ProductCategory.create(
            product_id=product_id, 
            category_id=category_id)
        new_product_categories.append(new_product_category)
    return new_product_categories

def load_product_suppliers(products_df: pd.DataFrame):
    new_product_suppliers = list()
    for i in tqdm.tqdm(range(len(products_df))):
        row = products_df.iloc[i]
        product_id = row['ProductID']
        supplier_id = row['SupplierID']
        new_product_supplier = models.ProductSupplier.create(
            product_id=product_id,
            supplier_id=supplier_id)
        new_product_suppliers.append(new_product_supplier)
    return new_product_suppliers

def load_shippers(shippers_df: pd.DataFrame):
    new_shippers = list()
    for i in tqdm.tqdm(range(len(shippers_df))):
        row = shippers_df.iloc[i]
        new_shipper = models.ShipperDim.create(
            shipper_id=row['ShipperID'],
            company_name=row['CompanyName'],
            phone=row['Phone'])
        new_shippers.append(new_shipper)
    return new_shippers

def load_territories(territories_df: pd.DataFrame):
    new_territories = list()
    for i in tqdm.tqdm(range(len(territories_df))):
        row = territories_df.iloc[i]
        region_descriptions = [None, "Eastern", "Western", "Northern", "Southern"]
        new_territory = models.TerritoryDim.create(
            territory_id=row['TerritoryID'],
            territory_description=row['TerritoryDescription'],
            region_description= region_descriptions[row['RegionID']])
        new_territories.append(new_territory)
    return new_territories

def load_employee_territories(employee_territories_df: pd.DataFrame):
    new_employee_territories = list()
    for i in tqdm.tqdm(range(len(employee_territories_df))):
        row = employee_territories_df.iloc[i]
        employee_id = row['EmployeeID']
        territory_id = row['TerritoryID']
        new_product_supplier = models.TerritoryEmployee.create(
            territory_id=territory_id,
            employee_id=employee_id)
        new_employee_territories.append(new_product_supplier)
    return new_employee_territories

def load_orders(orders_df: pd.DataFrame):
    print('Loading orders')
    new_orders = list()
    new_shipping_addresses = list()
    new_shipping_orders = list()
    new_shipping_deliveries = list()
    for i in tqdm.tqdm(range(len(orders_df))):
        row = orders_df.iloc[i]
        order_id = row['OrderID']
        customer = models.CustomerDim.get(models.CustomerDim.customer_id == row['CustomerID'])
        employee = models.EmployeeDim.get(models.EmployeeDim.employee_id == row['EmployeeID'])
        shipper  = models.ShipperDim.get(models.ShipperDim.shipper_id == row['ShipVia'])
        
        order_date = models.DateDim.create_from_timestamp( datetime.datetime.strptime(row['OrderDate'][:16], '%Y-%m-%d %H:%M'))
        order_fact = models.OrderFact.create(
            order_id=order_id,
            customer=customer,
            employee=employee,
            date=order_date,
            final_price=0)
        new_orders.append(order_fact)

        shipping_address = models.ShippingAddressDim.create(
            address=row['ShipAddress'],
            city=row['ShipCity'],
            region=row['ShipRegion'],
            postal_code=row['ShipPostalCode'],
            country=row['ShipCountry'])
        new_shipping_addresses.append(shipping_address)

        required_date = models.DateDim.create_from_timestamp( datetime.datetime.strptime(row['RequiredDate'][:16], '%Y-%m-%d %H:%M'))
        shipping_order_fact = models.ShippingOrderFact.create(
            address=shipping_address,
            order_date=order_date,
            required_date=required_date,
            order=order_fact,
            shipper=shipper)
        new_shipping_orders.append(shipping_order_fact)
        
        if not pd.isna(row['ShippedDate']):
            shipping_delivery_date = models.DateDim.create_from_timestamp( datetime.datetime.strptime(row['ShippedDate'][:16], '%Y-%m-%d %H:%M'))
            shipping_delivery_fact = models.ShippinggDeliveryFact.create(
                shipper=shipper,
                order=order_fact,
                address=shipping_address,
                delivery_date=shipping_delivery_date)
            new_shipping_deliveries.append(shipping_delivery_fact)
    
    print(f'{len(new_orders)} orders loaded')
    print(f'{len(new_shipping_addresses)} shipping addresses loaded')
    print(f'{len(new_shipping_orders)} shipping orders loaded')
    print(f'{len(new_shipping_deliveries)} shipping deliveries loaded')

def load_product_orders(product_order_df: pd.DataFrame):
    new_product_orders = list()
    for i in tqdm.tqdm(range(len(product_order_df))):
        row = product_order_df.iloc[i]
        new_product_order = models.ProductOrder.create(
            product_id=int(row['ProductID']),
            order_id=int(row['OrderID']),
            unit_price=row['UnitPrice'],
            quantity=row['Quantity'],
            discount=row['Discount'],
            final_price=row['UnitPrice']*row['Quantity']-row['Discount'])
        new_product_orders.append(new_product_order)

        order_fact = models.OrderFact.get(models.OrderFact.order_id == row['OrderID'])
        order_fact.final_price += new_product_order.final_price # <-- Update order pricing
        order_fact.save()
    return new_product_orders

if __name__ == '__main__':
    print('Loading products')
    products_data = pd.read_excel('data/Northwind.xlsx', sheet_name='Products')
    products = load_products(products_data)     # <-- load products records
    print(f'{len(products)} products loaded\n')
    
    print('Loading Categories')
    categories_data = pd.read_excel('data/Northwind.xlsx', sheet_name='Categories')
    categories = load_categories(categories_data)  # <-- load categories records
    print(f'{len(categories)} categories loaded\n')

    print('Loading Product Categories')
    product_categories = load_product_categories(products_data)  # <-- load product categories records
    print(f'{len(product_categories)} product categories loaded\n')

    print('Loading suppliers')
    suppliers_data = pd.read_excel('data/Northwind.xlsx', sheet_name='Suppliers')
    suppliers = load_suppliers(suppliers_data)  # <-- load suppliers records
    print(f'{len(suppliers)} suppliers loaded\n')

    print('Loading Product Suppliers')
    product_suppliers = load_product_suppliers(products_data)  # <-- load product suppliers records
    print(f'{len(product_suppliers)} product suppliers loaded\n')

    print('Loading customers')
    customers_data = pd.read_excel('data/Northwind.xlsx', sheet_name='Customers')
    customers = load_customers(customers_data)     # <-- load customers records
    print(f'{len(customers)} customers loaded\n')

    print('Loading employees')
    employees_data = pd.read_excel('data/Northwind.xlsx', sheet_name='Employees')
    employees = load_employees(employees_data)  # <-- load employees records
    print(f'{len(employees)} employees loaded\n')

    print('Loading shippers')
    shippers_data = pd.read_excel('data/Northwind.xlsx', sheet_name='Shippers')
    shippers = load_shippers(shippers_data)
    print(f'{len(shippers)} shippers loaded')
    
    print('Loading territories')
    territories_data = pd.read_excel('data/Northwind.xlsx', sheet_name='Territories')
    territories = load_territories(territories_data)
    print(f'{len(territories)} territories loaded')

    print('Loading employees territories')
    employee_territories_data = pd.read_excel('data/Northwind.xlsx', sheet_name='EmployeeTerritories')
    employee_territories = load_employee_territories(employee_territories_data)
    print(f'{len(employee_territories)} employee territories loaded')

    orders_data = pd.read_excel('data/Northwind.xlsx', sheet_name='Orders')
    load_orders(orders_data)

    print('Loading Product Orders')
    product_orders_data = pd.read_excel('data/Northwind.xlsx', sheet_name='Order Details')
    load_product_orders(product_orders_data)