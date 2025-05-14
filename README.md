# CENGFACTORYDB Java Application

A simple Java application that interacts with a MySQL database using JDBC. It performs CRUD operations on various tables such as factories, employees, and products.

## 📦 Features

- Connects to a MySQL database named `CENGFACTORY`
- Performs Create, Read, Update, and Delete operations on:
  - `factory`
  - `employee`
  - `product`
- Uses **prepared statements** for secure and efficient queries
- Demonstrates best practices for JDBC usage

## 🛠️ Technologies Used

- Java
- JDBC (Java Database Connectivity)
- MySQL

## ✅ Prerequisites

- Java JDK 11 or later
- MySQL Server running
- MySQL JDBC Driver (Connector/J)
- `CENGFACTORY` database created with necessary tables

## 🚀 Getting Started

1. **Clone the repository** or download the source files.
2. **Set up the MySQL database** with a schema named `CENGFACTORY` and create the required tables.
3. **Configure database credentials** in the `getConnection()` method inside `CENGFACTORYDB.java`.

   ```java
   String url = "jdbc:mysql://localhost:3306/CENGFACTORY";
   String username = "your_username";
   String password = "your_password";
