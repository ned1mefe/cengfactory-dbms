package ceng.ceng351.cengfactorydb;

import java.sql.Statement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class CENGFACTORYDB implements ICENGFACTORYDB{

    private static Connection connection = null;

    public void initialize() {
        String url = "jdbc:mysql://" + "144.122.71.128" + ":" + "8080" + "/" + "db2521326";
        try {
            connection = DriverManager.getConnection(url, "e2521326", "wEs01Z+roj9m");
        }
        catch (SQLException e)
        {
            System.out.println(e.toString());
        }
    }

    //This is for the count of the tables created and dropped when empty
    private int executeLine(String query)
    {
        try
        {
            Statement stmt = connection.createStatement();
            stmt.executeUpdate(query);
        }
        catch (SQLException e) {

            System.out.println(e.toString());
            return 0;
        }
        return 1;
    }

    public int createTables()
    {
        int success = 0;

        String FactoryQuery = "CREATE TABLE Factory ("
                + "factoryId INT PRIMARY KEY,"
                + "factoryName VARCHAR(50),"
                + "factoryType VARCHAR(50),"
                + "country VARCHAR(50))";


        String EmployeeQuery = "CREATE TABLE Employee ("
                + "employeeId INT PRIMARY KEY,"
                + "employeeName VARCHAR(50),"
                + "department VARCHAR(50),"
                + "salary INT)";


        String Works_InQuery = "CREATE TABLE Works_In ("
                + "factoryId INT,"
                + "employeeId INT,"
                + "startDate DATE,"
                + "PRIMARY KEY (factoryId, employeeId),"
                + "FOREIGN KEY (factoryId) REFERENCES Factory(factoryId) ON DELETE CASCADE ON UPDATE CASCADE,"
                + "FOREIGN KEY (employeeId) REFERENCES Employee(employeeId) ON DELETE CASCADE ON UPDATE CASCADE)";


        String ProductQuery = "CREATE TABLE Product ("
                + "productId INT PRIMARY KEY,"
                + "productName VARCHAR(50),"
                + "productType VARCHAR(50))";

        String ProduceQuery = "CREATE TABLE Produce ("
                + "factoryId INT,"
                + "productId INT,"
                + "amount INT,"
                + "productionCost INT,"
                + "PRIMARY KEY (factoryId, productId),"
                + "FOREIGN KEY (factoryId) REFERENCES Factory(factoryId) ON DELETE CASCADE ON UPDATE CASCADE,"
                + "FOREIGN KEY (productId) REFERENCES Product(productId) ON DELETE CASCADE ON UPDATE CASCADE)";

        String Shipment = "CREATE TABLE Shipment ("
                + "factoryId INT,"
                + "productId INT,"
                + "amount INT,"
                + "pricePerUnit INT,"
                + "PRIMARY KEY (factoryId, productId),"
                + "FOREIGN KEY (factoryId) REFERENCES Factory(factoryId) ON DELETE CASCADE ON UPDATE CASCADE,"
                + "FOREIGN KEY (productId) REFERENCES Product(productId) ON DELETE CASCADE ON UPDATE CASCADE)";


        success += executeLine(FactoryQuery);
        success += executeLine(EmployeeQuery);
        success += executeLine(Works_InQuery);
        success += executeLine(ProductQuery);
        success += executeLine(ProduceQuery);
        success += executeLine(Shipment);

        return success;
    }
    public int dropTables()
    {
        int success = 0;

        String dropFactory = "DROP TABLE IF EXISTS Factory;";
        String dropEmployee = "DROP TABLE IF EXISTS Employee;";
        String dropWorks_In = "DROP TABLE IF EXISTS Works_In;";
        String dropProduct = "DROP TABLE IF EXISTS Product;";
        String dropProduce = "DROP TABLE IF EXISTS Produce;";
        String dropShipment = "DROP TABLE IF EXISTS Shipment;";

        success += executeLine(dropWorks_In);
        success += executeLine(dropShipment);
        success += executeLine(dropProduce);
        success += executeLine(dropProduct);
        success += executeLine(dropFactory);
        success += executeLine(dropEmployee);

        return success;
    }

    public int insertFactory(Factory[] factories) {

        int success = 0;

        String query = "INSERT INTO Factory "
                + "VALUES(?, ?, ?, ?)";


        try
        {
            PreparedStatement prst = connection.prepareStatement(query);

            for (Factory factory : factories) {
                prst.setInt(1, factory.getFactoryId());
                prst.setString(2, factory.getFactoryName());
                prst.setString(3, factory.getFactoryType());
                prst.setString(4, factory.getCountry());

                success += prst.executeUpdate();
            }

        }
        catch (SQLException e)
        {
            System.out.println(e.toString());
        }

        return success;
    }

    public int insertEmployee(Employee[] employees) {
        int success = 0;

        String query = "INSERT INTO Employee "
                + "VALUES(?, ?, ?, ?)";

        try
        {
            PreparedStatement prst = connection.prepareStatement(query);

            for (Employee employee : employees) {
                prst.setInt(1, employee.getEmployeeId());
                prst.setString(2, employee.getEmployeeName());
                prst.setString(3, employee.getDepartment());
                prst.setInt(4, employee.getSalary());

                success += prst.executeUpdate();
            }
        }
        catch (SQLException e)
        {
            System.out.println(e.toString());
        }

        return success;
    }

    public int insertWorksIn(WorksIn[] worksIns) {
        int success = 0;

        String query = "INSERT INTO Works_In "
                + "VALUES(?, ?, ?)";

        try
        {
            PreparedStatement prst = connection.prepareStatement(query);

            for (WorksIn Works_In : worksIns) {
                prst.setInt(1, Works_In.getFactoryId());
                prst.setInt(2, Works_In.getEmployeeId());
                prst.setString(3, Works_In.getStartDate());

                success += prst.executeUpdate();
            }
        }catch (SQLException e)
        {
            System.out.println(e.toString());
        }

        return success;
    }

    public int insertProduct(Product[] products) {
        int success = 0;

        String query = "INSERT INTO Product "
                + "VALUES(?, ?, ?)";

        try
        {
            PreparedStatement prst = connection.prepareStatement(query);

            for (Product product : products) {
                prst.setInt(1, product.getProductId());
                prst.setString(2, product.getProductName());
                prst.setString(3, product.getProductType());

                success += prst.executeUpdate();
            }
        }catch (SQLException e)
        {
            System.out.println(e.toString());
        }
        return success;
    }

    public int insertProduce(Produce[] produces) {
        int success = 0;

        String query = "INSERT INTO Produce "
                + "VALUES(?, ?, ?, ?)";

        try
        {
            PreparedStatement prst = connection.prepareStatement(query);

            for (Produce produce : produces) {
                prst.setInt(1, produce.getFactoryId());
                prst.setInt(2, produce.getProductId());
                prst.setInt(3, produce.getAmount());
                prst.setInt(4, produce.getProductionCost());

                success += prst.executeUpdate();
            }
        }catch (SQLException e)
        {
            System.out.println(e.toString());
        }
        return success;
    }

    public int insertShipment(Shipment[] shipments) {
        int success = 0;

        String query = "INSERT INTO Shipment "
                + "VALUES(?, ?, ?, ?)";

        try {
            PreparedStatement prst = connection.prepareStatement(query);

            for (Shipment shipment : shipments) {
                prst.setInt(1, shipment.getFactoryId());
                prst.setInt(2, shipment.getProductId());
                prst.setInt(3, shipment.getAmount());
                prst.setInt(4, shipment.getPricePerUnit());

                success += prst.executeUpdate();
            }
        }
        catch (SQLException e)
        {
            System.out.println(e.toString());
        }

        return success;
    }

    // pretty much self-explanatory
    public Factory[] getFactoriesForGivenCountry(String country) {

        ArrayList<Factory> factories = new ArrayList<Factory>();

        String query = "SELECT * FROM Factory F WHERE F.country = ?"
                + " ORDER BY factoryId ASC";

        try(PreparedStatement prst = connection.prepareStatement(query))
        {
            prst.setString(1,country);
            ResultSet rs = prst.executeQuery();

            while (rs.next()) {
                Factory factory = new Factory(
                        rs.getInt("factoryId"),
                        rs.getString("factoryName"),
                        rs.getString("factoryType"),
                        rs.getString("country"));

                factories.add(factory);
            }
        }catch (SQLException e)
        {
            System.out.println(e.toString());
        }
        return factories.toArray(new Factory[0]);
    }

    // as name suggests, gives the factories without employees
    public Factory[] getFactoriesWithoutAnyEmployee() {
        ArrayList<Factory> factories = new ArrayList<Factory>();

        String query = "SELECT * "
                + "FROM Factory F "
                + "WHERE NOT EXISTS ("
                + "SELECT * "
                + "FROM Works_In W "
                + "WHERE F.factoryId = W.factoryId)"
                + " ORDER BY FactoryId ASC";

        try(Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(query)) {

            while (rs.next()) {
                Factory factory = new Factory(
                        rs.getInt("factoryId"),
                        rs.getString("factoryName"),
                        rs.getString("factoryType"),
                        rs.getString("country"));

                factories.add(factory);
            }
        }catch (SQLException e)
        {
            System.out.println(e.toString());
        }
        return factories.toArray(new Factory[0]);
    }

    // gives factories producing all products with given type
    public Factory[] getFactoriesProducingAllForGivenType(String productType) {
        ArrayList<Factory> factories = new ArrayList<Factory>();

        String query = "SELECT * "
                + "FROM Factory F "
                + "WHERE NOT EXISTS ("
                + "SELECT Pt1.productId "
                + "FROM Product Pt1 "
                + "WHERE Pt1.productType = ?"

                + " EXCEPT "

                + "SELECT Pt2.productId "
                + "FROM Product Pt2, Produce Pe "
                + "WHERE Pt2.productId = Pe.productId AND "
                + "Pe.factoryId = F.factoryId) "
                + "ORDER BY factoryId ASC";

        try(PreparedStatement prst = connection.prepareStatement(query))
        {
            prst.setString(1,productType);
            ResultSet rs = prst.executeQuery();

            while (rs.next()) {
                Factory factory = new Factory(
                        rs.getInt("factoryId"),
                        rs.getString("factoryName"),
                        rs.getString("factoryType"),
                        rs.getString("country"));

                factories.add(factory);
            }
        }catch (SQLException e)
        {
            System.out.println(e.toString());
        }
        return factories.toArray(new Factory[0]);
    }

    // gives products that are produced but not shipped from same factory
    public Product[] getProductsProducedNotShipped() {
        ArrayList<Product> products = new ArrayList<Product>();

        String query = "SELECT DISTINCT Pt.* "
                + "FROM Product Pt, Produce Pe "
                + "WHERE Pt.productId = Pe.productId AND NOT EXISTS( "
                + "SELECT S.productID "
                + "FROM Shipment S "
                + "WHERE S.factoryId = Pe.factoryId AND S.productId = Pe.productId) "
                + "ORDER BY productId ASC";

        try(Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(query)) {

            while (rs.next()) {
                Product product = new Product(
                        rs.getInt("productId"),
                        rs.getString("productName"),
                        rs.getString("productType"));

                products.add(product);
            }
        }catch (SQLException e)
        {
            System.out.println(e.toString());
        }
        return products.toArray(new Product[0]);
    }

    // gives the average salary of employees working in given factory's given department
    public double getAverageSalaryForFactoryDepartment(int factoryId, String department) {

        double average = 0;

        String query = "SELECT W.factoryId, AVG(E.salary) as 'AVERAGE' "
                + "FROM Employee E, Works_In W "
                + "WHERE E.employeeId = W.employeeId "
                + "AND W.factoryId = ? "
                + "AND E.department = ? "
                + "GROUP BY E.department";

        try(PreparedStatement prst = connection.prepareStatement(query))
        {
            prst.setInt(1,factoryId);
            prst.setString(2,department);

            ResultSet rs = prst.executeQuery();

            rs.next();
            average = rs.getDouble("AVERAGE");

        }catch (SQLException e)
        {
            System.out.println(e.toString());
        }

        return average;
    }

    // gives the most valuable products for each factory
    public QueryResult.MostValueableProduct[] getMostValueableProducts() {
        ArrayList<QueryResult.MostValueableProduct> products = new ArrayList<>();

        String query = "SELECT temp2.factoryId, temp2.productId, temp2.productName, temp2.productType, MAX(temp2.max) as maxprofit " +
                "FROM (SELECT temp.factoryId, " +
                "Pr.productId, " +
                "Pr.productName, " +
                "Pr.productType, " +
                "MAX(temp.profit) as max " +
                "FROM Product Pr, " +
                "(SELECT P.factoryId, P.productId, (IFNULL((S.amount * S.pricePerUnit),0) - P.amount * P.productionCost) AS profit " +
                "FROM Produce P " +
                "LEFT OUTER JOIN Shipment S " +
                "ON P.factoryId = S.factoryId AND P.productId = S.productId) AS temp " +
                "WHERE Pr.productId = temp.productId " +
                "GROUP BY temp.factoryId, temp.productId " +
                "ORDER BY temp.profit DESC, factoryId ASC) AS temp2 " +
                "GROUP BY temp2.factoryId " +
                "ORDER BY maxprofit DESC, factoryId ASC";

        try(Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(query)) {

            while (rs.next()) {
                QueryResult.MostValueableProduct MVP = new QueryResult.MostValueableProduct(
                        rs.getInt("factoryId"),
                        rs.getInt("productId"),
                        rs.getString("productName"),
                        rs.getString("productType"),
                        rs.getDouble("maxprofit"));

                products.add(MVP);
            }
        }catch (SQLException e)
        {
            System.out.println(e.toString());
        }
        return products.toArray(new QueryResult.MostValueableProduct[0]);
    }

    // gives the factories that gather most profit from each product
    public QueryResult.MostValueableProduct[] getMostValueableProductsOnFactory() {
        ArrayList<QueryResult.MostValueableProduct> products = new ArrayList<>();

        // same with the above function, only difference is the group by part
        String query = "SELECT temp2.factoryId, temp2.productId, temp2.productName, temp2.productType, MAX(temp2.max) as maxprofit " +
                "FROM (SELECT temp.factoryId, " +
                "Pr.productId, " +
                "Pr.productName, " +
                "Pr.productType, " +
                "MAX(temp.profit) as max " +
                "FROM Product Pr, " +
                "(SELECT P.factoryId, P.productId, (IFNULL((S.amount * S.pricePerUnit),0) - P.amount * P.productionCost) AS profit " +
                "FROM Produce P " +
                "LEFT OUTER JOIN Shipment S " +
                "ON P.factoryId = S.factoryId AND P.productId = S.productId) AS temp " +
                "WHERE Pr.productId = temp.productId " +
                "GROUP BY temp.factoryId, temp.productId " +
                "ORDER BY temp.profit DESC, factoryId ASC) AS temp2 " +
                "GROUP BY temp2.productId " +
                "ORDER BY maxprofit DESC, productId ASC";

        try(Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(query)) {

            while (rs.next()) {
                QueryResult.MostValueableProduct MVP = new QueryResult.MostValueableProduct(
                        rs.getInt("factoryId"),
                        rs.getInt("productId"),
                        rs.getString("productName"),
                        rs.getString("productType"),
                        rs.getDouble("maxprofit"));

                products.add(MVP);
            }
        }catch (SQLException e)
        {
            System.out.println(e.toString());
        }
        return products.toArray(new QueryResult.MostValueableProduct[0]);
    }

    // gives the employees that earn less than the average of their department
    public QueryResult.LowSalaryEmployees[] getLowSalaryEmployeesForDepartments() {

        ArrayList<QueryResult.LowSalaryEmployees> employees = new ArrayList<>();

        String query = "SELECT DISTINCT E.employeeId, E.employeeName, E.department, (IF(W.startDate IS NULL, 0, E.salary)) AS realSalary " +
                "FROM Employee E " +
                "         NATURAL LEFT OUTER JOIN Works_In W, " +
                "    (SELECT Em.department, AVG(IF(W.startDate IS NULL, 0, Em.salary)) AS AvgSalary " +
                "        FROM Employee Em " +
                "        NATURAL LEFT OUTER JOIN Works_In W " +
                "    GROUP BY Em.department) AS temp " +
                "WHERE E.department = temp.department AND " +
                "    IF(W.startDate IS NULL, 0, E.salary) < temp.AvgSalary " +
                "ORDER BY employeeId ASC";

        try(Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(query)) {

            while (rs.next()) {
                QueryResult.LowSalaryEmployees employee = new QueryResult.LowSalaryEmployees(
                        rs.getInt("employeeId"),
                        rs.getString("employeeName"),
                        rs.getString("department"),
                        rs.getInt("realSalary"));

                employees.add(employee);
            }
        }catch (SQLException e)
        {
            System.out.println(e.toString());
        }
        return employees.toArray(new QueryResult.LowSalaryEmployees[0]);
    }

    // increases cost of the given productType by a given percentage
    public int increaseCost(String productType, double percentage) {

        int success = 0;

        String query = "UPDATE Produce Pe "
                + "SET Pe.productionCost = Pe.productionCost*(? + 1) "
                + "WHERE EXISTS ("
                + "SELECT P.productId "
                + "FROM Product P "
                + "WHERE P.productType = ? "
                + "AND P.productId = Pe.productId)";
        try {
            PreparedStatement prst = connection.prepareStatement(query);

            prst.setDouble(1,percentage);
            prst.setString(2,productType);

            success = prst.executeUpdate();
        }
        catch (SQLException e)
        {
            System.out.println(e.toString());
        }

        return success;
    }

    // deletes the records of the employees who are not working since the given date
    public int deleteNotWorkingEmployees(String givenDate) {

        int success = 0;

        String query = "DELETE E " +
                "FROM Employee E " +
                "WHERE NOT EXISTS( " +
                "SELECT * " +
                "FROM Works_In W " +
                "WHERE (W.startDate IS NULL OR " +
                "W.startDate > ?) " +
                "AND W.employeeId = E.employeeId)";
        try {
            PreparedStatement prst = connection.prepareStatement(query);

            prst.setString(1,givenDate);

            success = prst.executeUpdate();
        }
        catch (SQLException e)
        {
            System.out.println(e.toString());
        }

        return success;

    }

}