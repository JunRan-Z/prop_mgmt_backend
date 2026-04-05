from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
from pydantic import BaseModel
from typing import Optional

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

PROJECT_ID = "mgmt545sp26"
DATASET = "property_mgmt"


# ------------------------------------------------------------
# Dependency: BigQuery client
# ------------------------------------------------------------
def get_bq_client():
    client = bigquery.Client(project=PROJECT_ID)
    try:
        yield client
    finally:
        client.close()


# ------------------------------------------------------------
# Pydantic models
# ------------------------------------------------------------
class PropertyCreate(BaseModel):
    name: str
    address: str
    city: str
    state: str
    postal_code: str
    property_type: str
    tenant_name: Optional[str] = None
    monthly_rent: float


class IncomeCreate(BaseModel):
    amount: float
    date: str
    description: Optional[str] = None


class ExpenseCreate(BaseModel):
    amount: float
    date: str
    category: str
    vendor: Optional[str] = None
    description: Optional[str] = None


# ------------------------------------------------------------
# Properties
# ------------------------------------------------------------
@app.get("/properties")
def get_properties(bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        ORDER BY property_id
    """
    try:
        results = bq.query(query).result()
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )


@app.get("/properties/{property_id}")
def get_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        results = list(bq.query(query, job_config=job_config).result())
        if not results:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Property not found"
            )
        return dict(results[0])
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )


@app.post("/properties")
def create_property(property_data: PropertyCreate, bq: bigquery.Client = Depends(get_bq_client)):
    id_query = f"""
        SELECT COALESCE(MAX(property_id), 0) + 1 AS next_id
        FROM `{PROJECT_ID}.{DATASET}.properties`
    """

    try:
        next_id_result = list(bq.query(id_query).result())
        next_id = next_id_result[0]["next_id"]

        insert_query = f"""
            INSERT INTO `{PROJECT_ID}.{DATASET}.properties`
            (property_id, name, address, city, state, postal_code, property_type, tenant_name, monthly_rent)
            VALUES
            (@property_id, @name, @address, @city, @state, @postal_code, @property_type, @tenant_name, @monthly_rent)
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("property_id", "INT64", next_id),
                bigquery.ScalarQueryParameter("name", "STRING", property_data.name),
                bigquery.ScalarQueryParameter("address", "STRING", property_data.address),
                bigquery.ScalarQueryParameter("city", "STRING", property_data.city),
                bigquery.ScalarQueryParameter("state", "STRING", property_data.state),
                bigquery.ScalarQueryParameter("postal_code", "STRING", property_data.postal_code),
                bigquery.ScalarQueryParameter("property_type", "STRING", property_data.property_type),
                bigquery.ScalarQueryParameter("tenant_name", "STRING", property_data.tenant_name),
                bigquery.ScalarQueryParameter("monthly_rent", "FLOAT64", property_data.monthly_rent),
            ]
        )

        bq.query(insert_query, job_config=job_config).result()

        return {
            "property_id": next_id,
            "name": property_data.name,
            "address": property_data.address,
            "city": property_data.city,
            "state": property_data.state,
            "postal_code": property_data.postal_code,
            "property_type": property_data.property_type,
            "tenant_name": property_data.tenant_name,
            "monthly_rent": property_data.monthly_rent,
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Insert failed: {str(e)}"
        )


@app.put("/properties/{property_id}")
def update_property(property_id: int, property_data: PropertyCreate, bq: bigquery.Client = Depends(get_bq_client)):
    check_query = f"""
        SELECT property_id
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """
    check_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        existing = list(bq.query(check_query, job_config=check_config).result())
        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Property not found"
            )

        update_query = f"""
            UPDATE `{PROJECT_ID}.{DATASET}.properties`
            SET
                name = @name,
                address = @address,
                city = @city,
                state = @state,
                postal_code = @postal_code,
                property_type = @property_type,
                tenant_name = @tenant_name,
                monthly_rent = @monthly_rent
            WHERE property_id = @property_id
        """

        update_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("property_id", "INT64", property_id),
                bigquery.ScalarQueryParameter("name", "STRING", property_data.name),
                bigquery.ScalarQueryParameter("address", "STRING", property_data.address),
                bigquery.ScalarQueryParameter("city", "STRING", property_data.city),
                bigquery.ScalarQueryParameter("state", "STRING", property_data.state),
                bigquery.ScalarQueryParameter("postal_code", "STRING", property_data.postal_code),
                bigquery.ScalarQueryParameter("property_type", "STRING", property_data.property_type),
                bigquery.ScalarQueryParameter("tenant_name", "STRING", property_data.tenant_name),
                bigquery.ScalarQueryParameter("monthly_rent", "FLOAT64", property_data.monthly_rent),
            ]
        )

        bq.query(update_query, job_config=update_config).result()

        return {
            "message": "Property updated successfully",
            "property_id": property_id
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Update failed: {str(e)}"
        )


@app.delete("/properties/{property_id}")
def delete_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    check_query = f"""
        SELECT property_id
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """
    check_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        existing = list(bq.query(check_query, job_config=check_config).result())
        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Property not found"
            )

        delete_query = f"""
            DELETE FROM `{PROJECT_ID}.{DATASET}.properties`
            WHERE property_id = @property_id
        """
        bq.query(delete_query, job_config=check_config).result()

        return {
            "message": "Property deleted successfully",
            "property_id": property_id
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Delete failed: {str(e)}"
        )


@app.get("/properties/{property_id}/summary")
def get_property_summary(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    property_query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """

    income_query = f"""
        SELECT COALESCE(SUM(amount), 0) AS total_income
        FROM `{PROJECT_ID}.{DATASET}.income`
        WHERE property_id = @property_id
    """

    expense_query = f"""
        SELECT COALESCE(SUM(amount), 0) AS total_expenses
        FROM `{PROJECT_ID}.{DATASET}.expenses`
        WHERE property_id = @property_id
    """

    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        property_result = list(bq.query(property_query, job_config=config).result())
        if not property_result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Property not found"
            )

        income_result = list(bq.query(income_query, job_config=config).result())
        expense_result = list(bq.query(expense_query, job_config=config).result())

        property_data = dict(property_result[0])
        property_data["total_income"] = income_result[0]["total_income"]
        property_data["total_expenses"] = expense_result[0]["total_expenses"]
        property_data["net_income"] = property_data["total_income"] - property_data["total_expenses"]

        return property_data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Summary query failed: {str(e)}"
        )


# ------------------------------------------------------------
# Income
# ------------------------------------------------------------
@app.get("/income/{property_id}")
def get_income(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT
            income_id,
            property_id,
            amount,
            date,
            description
        FROM `{PROJECT_ID}.{DATASET}.income`
        WHERE property_id = @property_id
        ORDER BY date DESC, income_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        results = bq.query(query, job_config=job_config).result()
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )


@app.post("/income/{property_id}")
def create_income(property_id: int, income_data: IncomeCreate, bq: bigquery.Client = Depends(get_bq_client)):
    check_query = f"""
        SELECT property_id
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """
    check_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    id_query = f"""
        SELECT COALESCE(MAX(income_id), 0) + 1 AS next_id
        FROM `{PROJECT_ID}.{DATASET}.income`
    """

    try:
        property_exists = list(bq.query(check_query, job_config=check_config).result())
        if not property_exists:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Property not found"
            )

        next_id_result = list(bq.query(id_query).result())
        next_id = next_id_result[0]["next_id"]

        insert_query = f"""
            INSERT INTO `{PROJECT_ID}.{DATASET}.income`
            (income_id, property_id, amount, date, description)
            VALUES
            (@income_id, @property_id, @amount, @date, @description)
        """

        insert_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("income_id", "INT64", next_id),
                bigquery.ScalarQueryParameter("property_id", "INT64", property_id),
                bigquery.ScalarQueryParameter("amount", "FLOAT64", income_data.amount),
                bigquery.ScalarQueryParameter("date", "DATE", income_data.date),
                bigquery.ScalarQueryParameter("description", "STRING", income_data.description),
            ]
        )

        bq.query(insert_query, job_config=insert_config).result()

        return {
            "income_id": next_id,
            "property_id": property_id,
            "amount": income_data.amount,
            "date": income_data.date,
            "description": income_data.description
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Insert failed: {str(e)}"
        )


# ------------------------------------------------------------
# Expenses
# ------------------------------------------------------------
@app.get("/expenses/{property_id}")
def get_expenses(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT
            expense_id,
            property_id,
            amount,
            date,
            category,
            vendor,
            description
        FROM `{PROJECT_ID}.{DATASET}.expenses`
        WHERE property_id = @property_id
        ORDER BY date DESC, expense_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        results = bq.query(query, job_config=job_config).result()
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )


@app.post("/expenses/{property_id}")
def create_expense(property_id: int, expense_data: ExpenseCreate, bq: bigquery.Client = Depends(get_bq_client)):
    check_query = f"""
        SELECT property_id
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """
    check_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    id_query = f"""
        SELECT COALESCE(MAX(expense_id), 0) + 1 AS next_id
        FROM `{PROJECT_ID}.{DATASET}.expenses`
    """

    try:
        property_exists = list(bq.query(check_query, job_config=check_config).result())
        if not property_exists:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Property not found"
            )

        next_id_result = list(bq.query(id_query).result())
        next_id = next_id_result[0]["next_id"]

        insert_query = f"""
            INSERT INTO `{PROJECT_ID}.{DATASET}.expenses`
            (expense_id, property_id, amount, date, category, vendor, description)
            VALUES
            (@expense_id, @property_id, @amount, @date, @category, @vendor, @description)
        """

        insert_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("expense_id", "INT64", next_id),
                bigquery.ScalarQueryParameter("property_id", "INT64", property_id),
                bigquery.ScalarQueryParameter("amount", "FLOAT64", expense_data.amount),
                bigquery.ScalarQueryParameter("date", "DATE", expense_data.date),
                bigquery.ScalarQueryParameter("category", "STRING", expense_data.category),
                bigquery.ScalarQueryParameter("vendor", "STRING", expense_data.vendor),
                bigquery.ScalarQueryParameter("description", "STRING", expense_data.description),
            ]
        )

        bq.query(insert_query, job_config=insert_config).result()

        return {
            "expense_id": next_id,
            "property_id": property_id,
            "amount": expense_data.amount,
            "date": expense_data.date,
            "category": expense_data.category,
            "vendor": expense_data.vendor,
            "description": expense_data.description
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Insert failed: {str(e)}"
        )