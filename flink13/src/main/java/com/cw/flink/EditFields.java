package com.cw.flink;

import java.io.*;


public class EditFields {
    public static void main(String[] args) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader("input/fields"));
            FileWriter writer = new FileWriter("input/out", false);
            FileWriter writer2 = new FileWriter("input/out2", false);
            String line = reader.readLine();
            while (line != null) {
                String newLine = line;

                switch (line) {
                    case "resowners":
                        newLine = "resOwners";
                        break;
                    case "objectversion":
                        newLine = "objectVersion";
                        break;
                    case "users":
                        newLine = "user";
                        break;
                    case "classname":
                        newLine = "className";
                        break;
                    case "createtime":
                        newLine = "createTime";
                        break;
                    case "outerobjectid":
                        newLine = "outerObjectId";
                        break;
                    case "tenantid":
                        newLine = "tenantId";
                        break;
                    case "updatetime":
                        newLine = "updateTime";
                        break;
                    case "classcode":
                        newLine = "classCode";
                        break;
                    case "ciid":
                        newLine = "ciId";
                        break;
                    case "onlyone":
                        newLine = "onlyOne";
                        break;
                    case "subclasscode":
                        newLine = "subClassCode";
                        break;
                    case "parentid":
                        newLine = "parentId";
                        break;
                }
                writer.write("value::json->>'" + newLine + "' as " + line + ",\n");
                writer2.write(line + ",\n");
                line = reader.readLine();
            }
            reader.close();
            writer.close();
            writer2.close();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
