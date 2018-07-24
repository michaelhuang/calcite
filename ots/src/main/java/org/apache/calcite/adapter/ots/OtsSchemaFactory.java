package org.apache.calcite.adapter.ots;

import org.apache.calcite.adapter.ots.metadata.TableConfigReader;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

/**
 * ots schemaFactory
 *
 * @author huangzf@thfund.com.cn
 * @version 1.0
 * @since 2018-07-13
 */
/*{
    "version": "1.0",
    "defaultSchema": "crm",
    "schemas": [
        {
        "name": "crm",
        "type": "custom",
        "factory": "org.apache.calcite.adapter.ots.OtsSchemaFactory",
            "operand": {
                "instanceName": "",
                "endPoint": "",
                "accessKeyId": "",
                "accessKeySecret": "",
                "directory": ""
            }
        }
    ]
  }*/
public class OtsSchemaFactory implements SchemaFactory {

    private final static String INSTANCENAME = "instanceName";
    private final static String ENDPOINT = "endPoint";
    private final static String ACCESSKEYID = "accessKeyId";
    private final static String ACCESSKEYSECRET = "accessKeySecret";
    private final static String DIRECTORY = "directory";

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        String instanceName = (String) operand.get(INSTANCENAME);
        String endPoint = (String) operand.get(ENDPOINT);
        String accessKeyId = (String) operand.get(ACCESSKEYID);
        String accessKeySecret = (String) operand.get(ACCESSKEYSECRET);
        String path =
            operand.get(ModelHandler.ExtraOperand.BASE_DIRECTORY.camelName) + File.separator
                + operand.get(DIRECTORY);
        Path dir = Paths.get(path);
        TableConfigReader.tableConfigs(dir);
        return new OtsSchema(instanceName, endPoint, accessKeyId, accessKeySecret);
    }
}
