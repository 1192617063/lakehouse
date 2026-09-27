import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.rest.RESTCatalogServer;

public class IcebergRestLauncher {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(conf);

        String principal = "iceberg/icebergrest.lakehouse.com@LAKEHOUSE.COM";
        String keytab = "/etc/security/keytabs/iceberg.service.keytab";
        UserGroupInformation.loginUserFromKeytab(principal, keytab);

        System.out.println("Kerberos login successful: " + UserGroupInformation.getCurrentUser());

        RESTCatalogServer.main(args);
    }
}
