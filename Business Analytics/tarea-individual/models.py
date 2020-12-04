import peewee as pw
import datetime
import os
import math

if os.path.exists('data/northwind.db'):
    os.remove("data/northwind.db") 

db = pw.SqliteDatabase('data/northwind.db')
class BaseModel(pw.Model):
    class Meta:
        database = db
db.connect()


## Modelos base de datos relacional con modelamiento dimensional

class DateDim(BaseModel):
    date = pw.DateField()
    day_name = pw.CharField()
    week_of_month = pw.IntegerField()
    month_number = pw.IntegerField()
    month_name = pw.CharField()
    year = pw.IntegerField()
    timezone = pw.CharField()
    time = pw.CharField()

    @classmethod
    def create_from_timestamp(cls, timestamp: datetime.datetime):
        dt = timestamp.date()
        first_day = dt.replace(day=1)
        dom = dt.day
        adjusted_dom = dom + first_day.weekday()
        week_of_month = int(math.ceil(adjusted_dom/7.0))

        return cls.create(
            date=timestamp.date(),
            day_name=timestamp.strftime('%A'),
            week_of_month=week_of_month,
            month_number=timestamp.month,
            month_name=timestamp.strftime('%B'),
            year=timestamp.year,
            timezone='UTC',
            time=timestamp.strftime('%H:%M'))



class ProductDim(BaseModel):
    product_id = pw.IntegerField()
    name = pw.CharField()
    description = pw.TextField(null=True)
    quantity_per_unit = pw.TextField(null=True)
    discontinued = pw.BooleanField(default=True)


class CustomerDim(BaseModel):
    customer_id = pw.CharField()
    company_name = pw.CharField()
    contact_name = pw.CharField()
    phone = pw.CharField()
    address = pw.CharField()
    region = pw.CharField()
    city = pw.CharField()
    country = pw.CharField()
    postal_code = pw.CharField()


class EmployeeDim(BaseModel):
    employee_id = pw.IntegerField()
    first_name = pw.CharField()
    last_name = pw.CharField()
    region = pw.CharField()
    city = pw.CharField()
    country = pw.CharField()
    phone = pw.CharField()
    address = pw.CharField()

class CategoryDim(BaseModel):
    category_id = pw.IntegerField()
    name = pw.CharField()
    description = pw.TextField()


class ProductCategory(BaseModel):
    product = pw.ForeignKeyField(ProductDim, backref='product_categories')
    category = pw.ForeignKeyField(CategoryDim, backref='product_categories')


class SupplierDim(BaseModel):
    supplier_id = pw.IntegerField()
    company_name = pw.CharField()
    contact_name = pw.CharField()
    address = pw.CharField()
    city = pw.CharField()
    region = pw.CharField()
    country = pw.CharField()
    postal_code = pw.CharField()
    phone = pw.CharField()
    fax = pw.CharField()


class ProductSupplier(BaseModel):
    product = pw.ForeignKeyField(ProductDim, backref='product_suppliers')
    supplier = pw.ForeignKeyField(SupplierDim, backref='product_suppliers')


class ShipperDim(BaseModel):
    shipper_id = pw.IntegerField()
    company_name = pw.CharField()
    phone = pw.CharField()


class OfficeDim(BaseModel):
    address = pw.TextField()
    region = pw.CharField()
    city = pw.CharField()
    country = pw.CharField()


class TerritoryDim(BaseModel):
    territory_id = pw.CharField()
    territory_description = pw.CharField()
    region_description = pw.CharField()


class TerritoryEmployee(BaseModel):
    employee = pw.ForeignKeyField(EmployeeDim, backref='territory_employees')
    territory = pw.ForeignKeyField(TerritoryDim, backref='territory_employees')


class ShippingAddressDim(BaseModel):
    address = pw.CharField()
    city = pw.CharField()
    region = pw.CharField()
    postal_code = pw.CharField()
    country = pw.CharField()


class OrderFact(BaseModel):
    order_id = pw.IntegerField()
    customer = pw.ForeignKeyField(CustomerDim, backref='order_details')
    employee = pw.ForeignKeyField(EmployeeDim, backref='order_details')
    date = pw.ForeignKeyField(DateDim, backref='order_facts')

class StockFact(BaseModel):
    date = pw.ForeignKeyField(DateDim, backref='stocks')
    product = pw.ForeignKeyField(ProductDim, backref='stocks')
    stock = pw.IntegerField()


class ProductOrder(BaseModel):
    product = pw.ForeignKeyField(ProductDim, backref='product_orders')
    unit_price = pw.FloatField()
    quantity = pw.IntegerField()
    discount = pw.FloatField()
    order = pw.ForeignKeyField(OrderFact, backref='product_orders')


class ShippingOrderFact(BaseModel):
    address = pw.ForeignKeyField(ShippingAddressDim, backref='shipping_orders')
    order_date = pw.ForeignKeyField(DateDim, backref='shipping_orders')
    required_date = pw.ForeignKeyField(DateDim, backref='shipping_orders')
    order = pw.ForeignKeyField(OrderFact, backref='shipping_orders')
    shipper = pw.ForeignKeyField(ShipperDim, backref='shipping_orders')


class ShippinggDeliveryFact(BaseModel):
    shipper = pw.ForeignKeyField(ShipperDim, backref='shipping_deliveries')
    order = pw.ForeignKeyField(OrderFact, backref='shipping_deliveries')
    address = pw.ForeignKeyField(ShippingAddressDim, backref='shipping_deliveries')
    delivery_date = pw.ForeignKeyField(DateDim, backref='shipping_deliveries')

db.create_tables([
    DateDim,
    ProductDim,
    CustomerDim,
    EmployeeDim,
    CategoryDim,
    ProductCategory,
    SupplierDim,
    ProductSupplier,
    ShipperDim,
    OfficeDim,
    TerritoryDim,
    TerritoryEmployee,
    ShippingAddressDim,
    OrderFact,
    StockFact,
    ProductOrder,
    ShippingOrderFact,
    ShippinggDeliveryFact,
])
