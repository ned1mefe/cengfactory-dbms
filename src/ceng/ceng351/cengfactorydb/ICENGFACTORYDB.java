package ceng.ceng351.cengfactorydb;

public interface ICENGFACTORYDB {

    /**
     * Place your initialization code inside if required.
     *
     * <p>
     * This function will be called before all other operations. If your implementation
     * need initialization , necessary operations should be done inside this function. For
     * example, you can set your connection to the database server inside this function.
     */
    public void initialize();

    /**
     * Should create the necessary tables when called.
     *
     * @return the number of tables that are created successfully.
     */
    public int createTables();

    /**
     * Should drop the tables if exists when called.
     *
     * @return the number of tables are dropped successfully.
     */
    public int dropTables();

    /**
     * Should insert an array of Factory into the database.
     *
     * @return Number of rows inserted successfully.
     */
    public int insertFactory(Factory[] factories);

    /**
     * Should insert an array of Employee into the database.
     *
     * @return Number of rows inserted successfully.
     */
    public int insertEmployee(Employee[] employees);

    /**
     * Should insert an array of WorksIn into the database.
     *
     * @return Number of rows inserted successfully.
     */
    public int insertWorksIn(WorksIn[] worksIns);

    /**
     * Should insert an array of Product into the database.
     *
     * @return Number of rows inserted successfully.
     */
    public int insertProduct(Product[] products);


    /**
     * Should insert an array of Produce into the database.
     *
     * @return Number of rows inserted successfully.
     */
    public int insertProduce(Produce[] produces);


    /**
     * Should insert an array of Shipment into the database.
     *
     * @return Number of rows inserted successfully.
     */
    public int insertShipment(Shipment[] shipments);

    /**
     * Should return all factories that are located in a particular country.
     *
     * @return Factory[]
     */
    public Factory[] getFactoriesForGivenCountry(String country);



    /**
     * Should return all factories without any working employees.
     *
     * @return Factory[]
     */
    public Factory[] getFactoriesWithoutAnyEmployee();

    /**
     * Should return all factories that produce all products for a particular productType
     *
     * @return Factory[]
     */
    public Factory[] getFactoriesProducingAllForGivenType(String productType);


    /**
     * Should return the products that are produced in a particular factory but
     * don’t have any shipment from that factory.
     *
     * @return Product[]
     */
    public Product[] getProductsProducedNotShipped();


    /**
     * For a given factoryId and department, should return the average salary of
     *     the employees working in that factory and that specific department.
     *
     * @return double
     */
    public double getAverageSalaryForFactoryDepartment(int factoryId, String department);


    /**
     * Should return the most profitable products for each factory
     *
     * @return QueryResult.MostValueableProduct[]
     */
    public QueryResult.MostValueableProduct[] getMostValueableProducts();


     /**
     * For each product, return the factories that gather the highest profit
     * for that product
     *
     * @return QueryResult.MostValueableProduct[]
     */
    public QueryResult.MostValueableProduct[] getMostValueableProductsOnFactory();


    /**
     * For each department, should return all employees that are paid under the
     *     average salary for that department. You consider the employees
     *     that do not work earning '0'.
     *
     * @return QueryResult.LowSalaryEmployees[]
     */
    public QueryResult.LowSalaryEmployees[] getLowSalaryEmployeesForDepartments();


    /**
     * For the products of given productType, increase the productionCost of every unit by a given percentage.
     *
     * @return number of rows affected
     */
    public int increaseCost(String productType, double percentage);


    /**
     * Deleting all employees that have not worked since the given date.
     *
     * @return number of rows affected
     */
    public int deleteNotWorkingEmployees(String givenDate);

}
