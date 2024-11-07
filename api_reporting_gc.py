from fastapi import HTTPException
from database_connection import DatabaseConnection

class APIReportingGC:
    def __init__(self):
        self.db_connection = DatabaseConnection()

    def get_employee_hires_by_quarter(self):
        
        query = """
            SELECT *
            FROM GlobantPoc.VW_HiresByDepartmentJobQuarter
            ORDER BY Department, Job;
        """
        connection = self.db_connection.connect()
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            columns = [column[0] for column in cursor.description]
            result = [dict(zip(columns, row)) for row in cursor.fetchall()]
            return result
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            cursor.close()
            connection.close()

    def get_departments_above_average_hires(self):
       
        query = """
            SELECT *
            FROM GlobantPoc.VW_DepartmentsAboveAverageHires
            ORDER BY hired DESC;
        """
        connection = self.db_connection.connect()
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            columns = [column[0] for column in cursor.description]
            result = [dict(zip(columns, row)) for row in cursor.fetchall()]
            return result
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            cursor.close()
            connection.close()
