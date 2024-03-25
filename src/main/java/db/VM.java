package db;

import sql.AST;

public class VM {
    public void evaluate(AST.Statement statement) {
        System.out.println(statement);
    }
}
