package org.shersfy.datahub.dbexecutor.connector.hadoop;

public enum HadoopAuthTypes {
    
    SIMPLE("simple"),
    KERBEROS("kerberos"),
    SENTRY("sentry");

    private String alias;

    HadoopAuthTypes(String alias){
        this.alias = alias;
    }

    public static HadoopAuthTypes indexOf(int index){
        switch (index) {
            case 1:
                return SIMPLE;
            case 2:
                return KERBEROS;
            case 3:
                return SENTRY;
            default:
                return SIMPLE;
        }
    }

    public String alias() {
        return alias;
    }

    public int index(){
        return this.ordinal()+1;
    }

}
