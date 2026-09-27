public class KerberosSqlClientWrapper {
    public static void main(String[] args) throws Exception {
        System.out.println("[Wrapper] Starting...");
        
        // 1. Flink Security 初始化
        Class<?> globalConfClass = Class.forName("org.apache.flink.configuration.GlobalConfiguration");
        Object flinkConf = globalConfClass.getMethod("loadConfiguration", String.class).invoke(null, "/opt/flink/conf");
        
        Class<?> secConfClass = Class.forName("org.apache.flink.runtime.security.SecurityConfiguration");
        Object secConf = secConfClass.getConstructor(flinkConf.getClass()).newInstance(flinkConf);
        
        Class<?> secUtilsClass = Class.forName("org.apache.flink.runtime.security.SecurityUtils");
        for (java.lang.reflect.Method m : secUtilsClass.getMethods()) {
            if (m.getName().equals("install") && m.getParameterCount() == 1) { m.invoke(null, secConf); break; }
        }
        
        Object secContext = null;
        for (java.lang.reflect.Method m : secUtilsClass.getMethods()) {
            if ((m.getName().contains("Context")) && m.getParameterCount() == 0) {
                Object tryCtx = m.invoke(null);
                if (tryCtx != null) { secContext = tryCtx; break; }
            }
        }
        System.out.println("[Wrapper] SecurityContext: " + secContext);
        
        // 2. 打印 SecurityContext 的所有方法，找到 runSecured
        if (secContext != null) {
            System.out.println("[Wrapper] Context methods:");
            for (java.lang.reflect.Method m : secContext.getClass().getMethods()) {
                System.out.println("  " + m.getName() + "(" + java.util.Arrays.toString(m.getParameterTypes()) + ") -> " + m.getReturnType());
            }
        }
        
        // 3. 直接跑（UGI 已经是 KERBEROS 了，直接 SqlClient.main）
        Class<?> ugiClass = Class.forName("org.apache.hadoop.security.UserGroupInformation");
        Object ugi = ugiClass.getMethod("getCurrentUser").invoke(null);
        System.out.println("[Wrapper] UGI: " + ugi);
        
        System.out.println("[Wrapper] Delegating to SqlClient.main()...");
        Class.forName("org.apache.flink.table.client.SqlClient").getMethod("main", String[].class)
            .invoke(null, (Object) args);
    }
}
