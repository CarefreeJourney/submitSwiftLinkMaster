/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.lcl.swiftlink.admin.test;


//public class UserTableShardingTest {
//
//    public static final String SQL = "CREATE TABLE `t_user_%d` (\n" +
//            "  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'ID',\n" +
//            "  `username` varchar(256) DEFAULT NULL COMMENT '用户名',\n" +
//            "  `password` varchar(512) DEFAULT NULL COMMENT '密码',\n" +
//            "  `real_name` varchar(256) DEFAULT NULL COMMENT '真实姓名',\n" +
//            "  `phone` varchar(128) DEFAULT NULL COMMENT '手机号',\n" +
//            "  `mail` varchar(512) DEFAULT NULL COMMENT '邮箱',\n" +
//            "  `deletion_time` bigint(20) DEFAULT NULL COMMENT '注销时间戳',\n" +
//            "  `create_time` datetime DEFAULT NULL COMMENT '创建时间',\n" +
//            "  `update_time` datetime DEFAULT NULL COMMENT '修改时间',\n" +
//            "  `del_flag` tinyint(1) DEFAULT NULL COMMENT '删除标识 0：未删除 1：已删除',\n" +
//            "  PRIMARY KEY (`id`),\n" +
//            "  UNIQUE KEY `idx_unique_username` (`username`) USING BTREE\n" +
//            ") ENGINE=InnoDB AUTO_INCREMENT=1715030926162935810 DEFAULT CHARSET=utf8mb4;";
//
//    public static void main(String[] args) {
//        for (int i = 0; i < 16; i++) {
//            System.out.printf((SQL) + "%n", i);
//        }
//    }
//}


//public class UserTableShardingTest {
//    public static void main(String[] args) {
//        List<Employee> employees = new ArrayList<>();
//        employees.add(new Employee("小明",2500));
//        employees.add(new Employee("小军",8000));
//        employees.add(new Employee("小红",100000));
//
//        for(Employee ele:employees){
//            ele.cal();
//        }
//    }
//}
//class Employee{
//    private String name;
//    private double salary;
//    public Employee(String name, double salary) {
//        this.name = name;
//        this.salary = salary;
//    }
//    public String getName() {
//        return name;
//    }
//
//    public double getSalary() {
//        return salary;
//    }
//    public void cal(){
//        double ans = salary - 3500;
//        if(ans<=0){
//            ans = 0;
//        }else if(ans<=1500){
//            ans = ans*0.03;
//        }else if(ans<4500){
//            ans = ans*0.1-105;
//        }else if(ans<9000){
//            ans = ans*0.2-555;
//        }else if(ans<35000){
//            ans = ans*0.25-1005;
//        }else if(ans<55000){
//            ans = ans*0.3-2755;
//        }else if(ans<80000){
//            ans = ans*0.35-5505;
//        }else{
//            ans = ans*0.45-13505;
//        }
//        System.out.println(name+"应该缴纳的个人所得税是："+String.format("%.1f",ans));
//    }
//}

class TreeNode {
    int val;
    TreeNode left;
    TreeNode right;
    TreeNode() {}
    TreeNode(int val) { this.val = val; }
    TreeNode(int val, TreeNode left, TreeNode right) {
        this.val = val;
        this.left = left;
        this.right = right;
    }
}
public class UserTableShardingTest {
    private static TreeNode buildTree(){
        TreeNode root = new TreeNode(1);
        root.left = new TreeNode(2);
        root.right = new TreeNode(5);
        root.left.left = new TreeNode(3,null,null);
        root.left.right = new TreeNode(4,null,null);
        root.right.left = null;
        root.right.right = new TreeNode(6);
        return root;
    }
    public static void main(String[] args){
        flatten(null);
    }
    public static void flatten(TreeNode root) {
        root = buildTree();
        root = myflatten(root);
    }
    private static TreeNode myflatten(TreeNode root){
        // 终止条件
        if(root==null) return null;
        System.out.println(String.format("root.val = %d",root.val));
        System.out.println("*************************************");
        TreeNode node = new TreeNode(root.val);
        node.right = myflatten(root.left);
        if(node.right!=null){
            System.out.println(String.format("node.right.val = %d",node.right.val));
            System.out.println("*************************************");
        }else {
            System.out.println("node.right==null");
        }
        TreeNode cur = node;
        while(cur.right!=null) { System.out.println(String.format("cur = %d",cur.val));cur = cur.right; }
        cur.right = myflatten(root.right);
        return node;
    }
}
