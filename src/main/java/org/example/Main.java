package org.example;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

import java.util.Scanner;

// 按两次 Shift 打开“随处搜索”对话框并输入 `show whitespaces`，
// 然后按 Enter 键。现在，您可以在代码中看到空格字符。
public class Main {
    public static void printHelp() {
        System.out.println("-------------Help---------------");
        System.out.println("input 'q' to quit");
        System.out.println("input 'h' to get help");
        System.out.println("input 'b' to check buffer");
        System.out.println("input 'r' to refresh buffer");
        System.out.println("input sql to exec");
        System.out.println("--------------------------------");
    }
    public static void main(String[] args) {
        // 自定义 zookeeper_hosts
        Client client = new Client("192.168.43.80:2181");
        System.out.println("Client init done!");
        System.out.println("Database status:");
        client.printBuffer();
        printHelp();

        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("\ncmd: ");
            String cmd = scanner.nextLine().trim().toLowerCase();

            if (cmd.equals("h")) {
                printHelp();
            } else if (cmd.equals("q")) {
                client.close();
                System.out.println("Client close.");
                break;
            } else if (cmd.equals("b")) {
                client.printBuffer();
            } else if (cmd.equals("r")) {
                client.getBuffer().refreshBuffer();
            } else {
                client.exec(cmd);
            }
        }
        scanner.close();
    }
}