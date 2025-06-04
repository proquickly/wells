"""
from this cmap:
Product	classified by	product type
Sales Metric	type of	Taxes
Sales Metric	type of	Deductions
Property	contains	quantity of wells
product type	type of	oil
product type	type of	Condensate
product type	type of	gas
Product	????	Property Product Type production Month
Property	produces	Product
Differential	compared to	Realised Price
Sales Metric	type of	Differential
Property	produces	Property Product Type production Month
Sales Metric	type of	Volume
Property Product Type production Month	measured by	Production Metrics
Product	????	Benchmark
Taxes	type of	Production Tax
Property Product Type production Month	described by	Sales Metric
product type	type of	ngl
Property Product Type production Month	????	NGL Yield
Taxes	type of	PropertyTax
Sales Metric	type of	Realised Price
Differential	vs	Benchmark
Sales Metric	type of	Revenue

1. generate me a database using sql by the data described above
2. make the sql runnable in python using sqlalchemy with a sqlite3 database
3. add code to generate significant volumes of test data across all tables
"""

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    ForeignKey,
    Date,
    UniqueConstraint,
    Table,
    MetaData,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from datetime import datetime, timedelta
import random
import string
import numpy as np
from tqdm import tqdm

# Create SQLite database engine
engine = create_engine(
    "sqlite:///../../data/oil_gas_production.db", echo=False
)  # Set echo to False to reduce console output when generating data
Base = declarative_base()

# Define association tables for many-to-many relationships
property_product = Table(
    "property_product",
    Base.metadata,
    Column(
        "property_id", Integer, ForeignKey("property.id"), primary_key=True
    ),
    Column("product_id", Integer, ForeignKey("product.id"), primary_key=True),
)


# Define models
class ProductType(Base):
    __tablename__ = "product_type"

    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False, unique=True)
    description = Column(String(255))

    products = relationship("Product", back_populates="product_type")
    pptpms = relationship(
        "PropertyProductTypeProductionMonth", back_populates="product_type"
    )


class Benchmark(Base):
    __tablename__ = "benchmark"

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    description = Column(String(255))

    products = relationship("Product", back_populates="benchmark")
    differentials = relationship("Differential", back_populates="benchmark")


class Product(Base):
    __tablename__ = "product"

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    product_type_id = Column(
        Integer, ForeignKey("product_type.id"), nullable=False
    )
    benchmark_id = Column(Integer, ForeignKey("benchmark.id"))
    description = Column(String(255))

    product_type = relationship("ProductType", back_populates="products")
    benchmark = relationship("Benchmark", back_populates="products")
    properties = relationship(
        "Property", secondary=property_product, back_populates="products"
    )


class Property(Base):
    __tablename__ = "property"

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    quantity_of_wells = Column(Integer, default=0)
    location = Column(String(255))
    description = Column(String(255))

    products = relationship(
        "Product", secondary=property_product, back_populates="properties"
    )
    pptpms = relationship(
        "PropertyProductTypeProductionMonth", back_populates="property"
    )


class ProductionMonth(Base):
    __tablename__ = "production_month"

    id = Column(Integer, primary_key=True)
    month_date = Column(Date, nullable=False, unique=True)
    month_name = Column(
        String(20)
    )  # SQLite doesn't support generated columns, so we'll handle this in code

    pptpms = relationship(
        "PropertyProductTypeProductionMonth", back_populates="production_month"
    )


class PropertyProductTypeProductionMonth(Base):
    __tablename__ = "property_product_type_production_month"

    id = Column(Integer, primary_key=True)
    property_id = Column(Integer, ForeignKey("property.id"), nullable=False)
    product_type_id = Column(
        Integer, ForeignKey("product_type.id"), nullable=False
    )
    production_month_id = Column(
        Integer, ForeignKey("production_month.id"), nullable=False
    )
    ngl_yield = Column(Float)

    property = relationship("Property", back_populates="pptpms")
    product_type = relationship("ProductType", back_populates="pptpms")
    production_month = relationship("ProductionMonth", back_populates="pptpms")
    production_metrics = relationship(
        "ProductionMetric", back_populates="pptpm"
    )
    sales_metrics = relationship("SalesMetric", back_populates="pptpm")

    __table_args__ = (
        UniqueConstraint(
            "property_id", "product_type_id", "production_month_id"
        ),
    )


class ProductionMetric(Base):
    __tablename__ = "production_metric"

    id = Column(Integer, primary_key=True)
    pptpm_id = Column(
        Integer,
        ForeignKey("property_product_type_production_month.id"),
        nullable=False,
    )
    metric_name = Column(String(50), nullable=False)
    metric_value = Column(Float, nullable=False)
    description = Column(String(255))

    pptpm = relationship(
        "PropertyProductTypeProductionMonth",
        back_populates="production_metrics",
    )


class SalesMetricType(Base):
    __tablename__ = "sales_metric_type"

    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False, unique=True)
    description = Column(String(255))

    sales_metrics = relationship(
        "SalesMetric", back_populates="sales_metric_type"
    )


class TaxType(Base):
    __tablename__ = "tax_type"

    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False, unique=True)
    description = Column(String(255))

    sales_metrics = relationship("SalesMetric", back_populates="tax_type")


class SalesMetric(Base):
    __tablename__ = "sales_metric"

    id = Column(Integer, primary_key=True)
    pptpm_id = Column(
        Integer,
        ForeignKey("property_product_type_production_month.id"),
        nullable=False,
    )
    sales_metric_type_id = Column(
        Integer, ForeignKey("sales_metric_type.id"), nullable=False
    )
    tax_type_id = Column(Integer, ForeignKey("tax_type.id"))
    value = Column(Float, nullable=False)
    description = Column(String(255))

    pptpm = relationship(
        "PropertyProductTypeProductionMonth", back_populates="sales_metrics"
    )
    sales_metric_type = relationship(
        "SalesMetricType", back_populates="sales_metrics"
    )
    tax_type = relationship("TaxType", back_populates="sales_metrics")
    realised_price_differentials = relationship(
        "Differential",
        back_populates="realised_price",
        foreign_keys="Differential.realised_price_id",
    )


class Differential(Base):
    __tablename__ = "differential"

    id = Column(Integer, primary_key=True)
    benchmark_id = Column(Integer, ForeignKey("benchmark.id"), nullable=False)
    realised_price_id = Column(
        Integer, ForeignKey("sales_metric.id"), nullable=False
    )
    value = Column(Float, nullable=False)
    description = Column(String(255))

    benchmark = relationship("Benchmark", back_populates="differentials")
    realised_price = relationship(
        "SalesMetric",
        back_populates="realised_price_differentials",
        foreign_keys=[realised_price_id],
    )


# Create all tables
Base.metadata.create_all(engine)

# Create a session
Session = sessionmaker(bind=engine)
session = Session()


# Helper functions for generating test data
def random_string(length=10):
    return "".join(random.choice(string.ascii_letters) for _ in range(length))


def random_description():
    return random_string(random.randint(20, 100))


def random_date(start_date, end_date):
    days_between = (end_date - start_date).days
    return start_date + timedelta(days=random.randint(0, days_between))


def random_float(min_val, max_val, decimals=2):
    return round(random.uniform(min_val, max_val), decimals)


# Parameters for data generation
NUM_PROPERTIES = 50
NUM_PRODUCTS = 30
NUM_BENCHMARKS = 10
NUM_MONTHS = 36  # 3 years of monthly data
NUM_PRODUCTION_METRICS_PER_PPTPM = 5
NUM_SALES_METRICS_PER_PPTPM = 5


# Add initial data
def initialize_database():
    print("Initializing database with reference data...")

    # Add product types
    product_types = [
        ProductType(name="oil", description="Crude oil products"),
        ProductType(name="gas", description="Natural gas products"),
        ProductType(name="condensate", description="Gas condensate products"),
        ProductType(name="ngl", description="Natural gas liquids"),
    ]
    session.add_all(product_types)

    # Add sales metric types
    sales_metric_types = [
        SalesMetricType(name="Taxes", description="Various tax payments"),
        SalesMetricType(
            name="Deductions", description="Contractual deductions"
        ),
        SalesMetricType(
            name="Differential", description="Price differentials"
        ),
        SalesMetricType(name="Volume", description="Sales volumes"),
        SalesMetricType(
            name="Realised Price", description="Actual sale prices"
        ),
        SalesMetricType(name="Revenue", description="Generated revenue"),
    ]
    session.add_all(sales_metric_types)

    # Add tax types
    tax_types = [
        TaxType(name="Production Tax", description="Tax on production volume"),
        TaxType(name="Property Tax", description="Tax on property value"),
    ]
    session.add_all(tax_types)

    session.commit()
    print("Database initialized with reference data.")


def generate_test_data():
    print("Generating large volume of test data...")

    # Get product types
    product_types = session.query(ProductType).all()

    # Generate benchmarks
    print("Generating benchmarks...")
    benchmarks = []
    for i in range(NUM_BENCHMARKS):
        benchmark = Benchmark(
            name=f"Benchmark {i + 1}",
            description=f"Description for benchmark {i + 1}",
        )
        benchmarks.append(benchmark)
    session.add_all(benchmarks)
    session.commit()

    # Generate products
    print("Generating products...")
    products = []
    for i in range(NUM_PRODUCTS):
        product = Product(
            name=f"Product {i + 1}",
            product_type_id=random.choice(product_types).id,
            benchmark_id=random.choice(benchmarks).id,
            description=random_description(),
        )
        products.append(product)
    session.add_all(products)
    session.commit()

    # Generate properties
    print("Generating properties...")
    properties = []
    locations = [
        "Texas",
        "Oklahoma",
        "Louisiana",
        "North Dakota",
        "Wyoming",
        "Colorado",
        "New Mexico",
        "California",
        "Alaska",
    ]
    for i in range(NUM_PROPERTIES):
        property = Property(
            name=f"Property {i + 1}",
            quantity_of_wells=random.randint(1, 100),
            location=random.choice(locations),
            description=random_description(),
        )
        properties.append(property)
    session.add_all(properties)
    session.commit()

    # Associate products with properties
    print("Associating products with properties...")
    for property in properties:
        # Each property has 1-5 products
        for product in random.sample(products, random.randint(1, 5)):
            property.products.append(product)
    session.commit()

    # Generate production months
    print("Generating production months...")
    start_date = datetime(2020, 1, 1)
    production_months = []
    for i in range(NUM_MONTHS):
        month_date = start_date + timedelta(days=30 * i)
        month = ProductionMonth(
            month_date=month_date, month_name=month_date.strftime("%Y-%m")
        )
        production_months.append(month)
    session.add_all(production_months)
    session.commit()

    # Generate PropertyProductTypeProductionMonth entries
    print("Generating PropertyProductTypeProductionMonth entries...")
    pptpms = []
    # We'll create entries for each property, for each product type, for each month
    # Using batch processing to avoid memory issues
    batch_size = 1000
    total_iterations = (
        len(properties) * len(product_types) * len(production_months)
    )

    print(
        f"Generating {total_iterations} PPTPM entries in batches of {batch_size}..."
    )

    for property_idx, property in enumerate(properties):
        for pt_idx, product_type in enumerate(product_types):
            for pm_idx, production_month in enumerate(production_months):
                # Only create PPTPM if the property has a product of this type
                property_product_types = set(
                    [p.product_type_id for p in property.products]
                )
                if (
                    product_type.id in property_product_types
                    or random.random() < 0.2
                ):  # 20% chance to add even if no matching product
                    ngl_yield = None
                    if product_type.name == "ngl":
                        ngl_yield = random_float(0.1, 5.0)

                    pptpm = PropertyProductTypeProductionMonth(
                        property_id=property.id,
                        product_type_id=product_type.id,
                        production_month_id=production_month.id,
                        ngl_yield=ngl_yield,
                    )
                    pptpms.append(pptpm)

                    # Batch commit to save memory
                    if len(pptpms) >= batch_size:
                        session.add_all(pptpms)
                        session.commit()
                        pptpms = []

    # Add any remaining PPTPMs
    if pptpms:
        session.add_all(pptpms)
        session.commit()

    # Generate production metrics and sales metrics for each PPTPM
    print("Generating production metrics and sales metrics...")

    # Get all PPTPMs from the database in batches to avoid memory issues
    pptpm_count = session.query(PropertyProductTypeProductionMonth).count()
    batch_size = 500
    num_batches = (pptpm_count // batch_size) + 1

    sales_metric_types = session.query(SalesMetricType).all()
    tax_types = session.query(TaxType).all()

    print(f"Processing {pptpm_count} PPTPMs in {num_batches} batches...")

    for batch_num in range(num_batches):
        offset = batch_num * batch_size
        batch_pptpms = (
            session.query(PropertyProductTypeProductionMonth)
            .offset(offset)
            .limit(batch_size)
            .all()
        )

        if not batch_pptpms:
            break

        production_metrics = []
        sales_metrics = []

        for pptpm in batch_pptpms:
            # Generate production metrics
            metric_names = [
                "Daily Rate",
                "Cumulative Production",
                "Peak Rate",
                "Decline Rate",
                "Water Cut",
            ]
            for _ in range(NUM_PRODUCTION_METRICS_PER_PPTPM):
                metric_name = random.choice(metric_names)
                production_metrics.append(
                    ProductionMetric(
                        pptpm_id=pptpm.id,
                        metric_name=metric_name,
                        metric_value=random_float(100, 10000, 2),
                        description=f"{metric_name} for {pptpm.property.name}",
                    )
                )

            # Generate sales metrics
            for sales_metric_type in sales_metric_types:
                tax_type_id = None
                if sales_metric_type.name == "Taxes":
                    tax_type_id = random.choice(tax_types).id

                value = random_float(10, 1000, 2)
                if sales_metric_type.name == "Volume":
                    value = random_float(1000, 100000, 2)
                elif sales_metric_type.name == "Revenue":
                    value = random_float(10000, 1000000, 2)

                sales_metrics.append(
                    SalesMetric(
                        pptpm_id=pptpm.id,
                        sales_metric_type_id=sales_metric_type.id,
                        tax_type_id=tax_type_id,
                        value=value,
                        description=f"{sales_metric_type.name} for {pptpm.property.name}",
                    )
                )

        # Add production metrics and sales metrics
        session.add_all(production_metrics)
        session.add_all(sales_metrics)
        session.commit()

        print(f"Processed batch {batch_num + 1}/{num_batches} of PPTPMs")

    # Generate differentials
    print("Generating differentials...")

    # Get all realised price sales metrics
    realised_price_type_id = (
        session.query(SalesMetricType.id)
        .filter(SalesMetricType.name == "Realised Price")
        .scalar()
    )

    # Process in batches
    batch_size = 1000
    realised_price_count = (
        session.query(SalesMetric)
        .filter(SalesMetric.sales_metric_type_id == realised_price_type_id)
        .count()
    )
    num_batches = (realised_price_count // batch_size) + 1

    print(
        f"Processing {realised_price_count} realized prices in {num_batches} batches..."
    )

    for batch_num in range(num_batches):
        offset = batch_num * batch_size
        realised_prices = (
            session.query(SalesMetric)
            .filter(SalesMetric.sales_metric_type_id == realised_price_type_id)
            .offset(offset)
            .limit(batch_size)
            .all()
        )

        if not realised_prices:
            break

        differentials = []
        for realised_price in realised_prices:
            pptpm = realised_price.pptpm
            product_benchmark_ids = []

            # Find relevant benchmarks for this property's products
            for product in pptpm.property.products:
                if (
                    product.benchmark_id
                    and product.product_type_id == pptpm.product_type_id
                ):
                    product_benchmark_ids.append(product.benchmark_id)

            # If no relevant benchmarks, just pick a random one
            if not product_benchmark_ids and benchmarks:
                product_benchmark_ids = [random.choice(benchmarks).id]

            # Create differential for each relevant benchmark
            for benchmark_id in product_benchmark_ids:
                differentials.append(
                    Differential(
                        benchmark_id=benchmark_id,
                        realised_price_id=realised_price.id,
                        value=random_float(-20, 20, 2),
                        description=f"Differential for {pptpm.property.name}",
                    )
                )

        session.add_all(differentials)
        session.commit()

        print(
            f"Processed batch {batch_num + 1}/{num_batches} of differentials"
        )

    print("Data generation complete!")


def analyze_database():
    """Print statistics about the database"""
    print("\nDatabase Statistics:")
    print(f"Properties: {session.query(Property).count()}")
    print(f"Products: {session.query(Product).count()}")
    print(f"Benchmarks: {session.query(Benchmark).count()}")
    print(f"Production Months: {session.query(ProductionMonth).count()}")
    print(
        f"PropertyProductTypeProductionMonth entries: {session.query(PropertyProductTypeProductionMonth).count()}"
    )
    print(f"Production Metrics: {session.query(ProductionMetric).count()}")
    print(f"Sales Metrics: {session.query(SalesMetric).count()}")
    print(f"Differentials: {session.query(Differential).count()}")

    # Sample property with its products
    sample_property = session.query(Property).first()
    if sample_property:
        print(f"\nSample Property: {sample_property.name}")
        print(f"  Wells: {sample_property.quantity_of_wells}")
        print(
            f"  Products: {', '.join([p.name for p in sample_property.products])}"
        )

        # Get a sample month of production for this property
        sample_pptpm = (
            session.query(PropertyProductTypeProductionMonth)
            .filter(
                PropertyProductTypeProductionMonth.property_id
                == sample_property.id
            )
            .first()
        )

        if sample_pptpm:
            print(f"\nSample Month Production:")
            print(f"  Month: {sample_pptpm.production_month.month_name}")
            print(f"  Product Type: {sample_pptpm.product_type.name}")

            # Get production metrics
            prod_metrics = (
                session.query(ProductionMetric)
                .filter(ProductionMetric.pptpm_id == sample_pptpm.id)
                .all()
            )

            if prod_metrics:
                print("  Production Metrics:")
                for metric in prod_metrics:
                    print(f"    {metric.metric_name}: {metric.metric_value}")

            # Get sales metrics
            sales_metrics = (
                session.query(SalesMetric)
                .filter(SalesMetric.pptpm_id == sample_pptpm.id)
                .all()
            )

            if sales_metrics:
                print("  Sales Metrics:")
                for metric in sales_metrics:
                    print(
                        f"    {metric.sales_metric_type.name}: {metric.value}"
                    )


if __name__ == "__main__":
    try:
        # Check if database already has data
        if session.query(ProductType).count() == 0:
            initialize_database()

        # Check if we need to generate test data
        if session.query(Property).count() < NUM_PROPERTIES:
            generate_test_data()
        else:
            print("Test data already exists. Skipping generation.")

        # Analyze the database
        analyze_database()

        print("\nDatabase created and initialized successfully!")

    except Exception as e:
        print(f"Error: {e}")
        session.rollback()
    finally:
        session.close()
