package client;

import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;

import java.util.Collections;
import java.util.Random;

public class exp {
    // random application name
    private static String getRandomString(int length) {
        String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random random = new Random();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(62);
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }

    private static ContainerLaunchContext createApp(YarnClient rmClient, String cmd)
            throws Exception {
        YarnClientApplication newApp = rmClient.createApplication();
        ApplicationId appId = newApp.getNewApplicationResponse().getApplicationId();
        // Create launch context for app master
        ApplicationSubmissionContext appContext = Records.newRecord(ApplicationSubmissionContext.class);
        // set the application id
        appContext.setApplicationId(appId);
        // set the application name
        appContext.setApplicationName(getRandomString(20));
        // Set the priority for the application master
        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(-1);
        appContext.setPriority(pri);
        // Set the queue to which this application is to be submitted in the RM
        appContext.setQueue("default");

        // Set up the container launch context for the application master
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

        amContainer.setCommands(Collections.singletonList(cmd));
        appContext.setResource(Resource.newInstance(1024, 1));
        appContext.setAMContainerSpec(amContainer);
        appContext.setApplicationType("YARN");
        appContext.setUnmanagedAM(false);
        // Submit application
        rmClient.submitApplication(appContext);

        return amContainer;
    }

    public static void main(String[] args)
            throws Exception {
        String rpc_host = args[0];
        String cmd = args[1];
        YarnConfiguration conf = new YarnConfiguration();
        YarnClient yarnClient = YarnClient.createYarnClient();

        conf.set("yarn.resourcemanager.address", rpc_host);

        System.setProperty("HADOOP_USER_NAME", getRandomString(5));
        yarnClient.init(conf);
        yarnClient.start();
        ContainerLaunchContext am = createApp(yarnClient, cmd);

        yarnClient.stop();
    }
}
