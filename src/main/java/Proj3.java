
import com.google.gson.JsonObject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.PushCommand;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class Proj3 {
    static Configuration configuration;
    static FileSystem fileSystem;
    static String CSV = "csv";
    static String PY = "py";
    static Git git;

  public static void main(String[] args) throws InvocationTargetException, IllegalAccessException, NoSuchMethodException, IOException, URISyntaxException, InterruptedException {
      git = Git.open(new File(System.getenv("airflow dag")));
      configuration = new Configuration();
      configuration.set("dfs.client.use.datanode.hostname", "true");
    System.out.println("Start to connect HDFS");
      fileSystem = FileSystem.get(new URI("hdfs://" + System.getenv("host") + ":9000"), configuration, "root");
    System.out.println("HDFS connected");
      Set<String> methods = new HashSet<String>() {{add("writeData"); add("read"); add("trigger");add("writeScript");add("writeDag");}};
    while (true) {
        Scanner sc = new Scanner(System.in);
        promptMethod();
        String method = sc.next();
        if (!methods.contains(method)) {
            promptWrongMethod();
        } else {
            Method md = Proj3.class.getDeclaredMethod(method);
            md.setAccessible(true);
            md.invoke(null);
            md.setAccessible(false);
        }
    }
  }

  private static void promptMethod() {
    System.out.println("If you want to write new data into application, please enter 'writeData'");
    System.out.println("If you want to read result from application, please enter 'read'");
    System.out.println("If you want to rerun the analysis to update your result, please enter 'trigger'");
    System.out.println("If you want to update or add a new analysis script into application, please enter 'writeScript'");
    System.out.println("If you want to update or add a new dag into application, please enter 'writeDag'");
    System.out.println("An analysis will be run automatically each day.");
  }

  private static void promptWrongMethod() {
    System.out.println("Please input a valid method name");
  }

  private static void writeScript() {
      try{
          Scanner sc = new Scanner(System.in);
          System.out.println("Please enter the local file/directory URL");
          System.out.println("For example: /home/tty/spark.py");
          System.out.println("If you input a directory URL, all valid files under the directory will be submitted");
          System.out.println("If a name collision happens, your file will take place of the original one.");
          String filePath = sc.next();
          File file = new File(filePath);
          traverseFolder(file, PY, "remote script");
          promptSucceed();
      } catch (IOException exception) {
          promptFailed(exception);
      }
  }

  private static void writeData() {
      try{
          Scanner sc = new Scanner(System.in);
          System.out.println("Please enter the local file/directory URL");
          System.out.println("For example: /home/tty/data.csv");
          System.out.println("If you input a directory URL, all valid files under the directory will be submitted");
          System.out.println("If a name collision happens, your file will take place of the original one.");
          String filePath = sc.next();
          File file = new File(filePath);
          traverseFolder(file, CSV, "remote data");
          promptSucceed();
      } catch (IOException exception) {
          promptFailed(exception);
      }
  }

    private static void writeDag() throws IOException, GitAPIException {
            Scanner sc = new Scanner(System.in);
            System.out.println("Please enter the local file/directory URL");
            System.out.println("For example: /home/tty/dag.py");
            System.out.println("If you input a directory URL, all valid files under the directory will be submitted");
            System.out.println("If a name collision happens, your file will take place of the original one.");
            String filePath = sc.next();
            File file = new File(filePath);
            traverseDag(file, PY);
            git.add().addFilepattern(".").call();
    System.out.println("Changes added");
            git.commit().setMessage(LocalDateTime.now().toString()).call();
    System.out.println("Changes commited");
            PushCommand pushCommand = git.push();
            pushCommand.setCredentialsProvider(new UsernamePasswordCredentialsProvider(System.getenv("username"), System.getenv("password")));
    System.out.println("Start to push");
            pushCommand.call();
    System.out.println("Changes pushed");
            promptSucceed();
    }

    private static void traverseDag(File file, String type) throws IOException {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (File f : files) {
                traverseDag(f, type);
            }
        } else {
            String mimeType = FilenameUtils.getExtension(file.getName());
            if (mimeType.equals(type)) {
                System.out.println("Start to copy file " + file.getName());
                FileUtils.copyFile(file, new File(System.getenv("airflow dag") + File.separator + file.getName()));
        System.out.println("File " + file.getName() + " copied");
            }
        }
    }

  private static void traverseFolder(File file, String type, String target) throws IOException {
      if (file.isDirectory()) {
          File[] files = file.listFiles();
          for (File f : files) {
              traverseFolder(f, type, target);
          }
      } else if (file.isFile()){
          String mimeType = FilenameUtils.getExtension(file.getName());
      if (mimeType.equals(type)) {
//          try{
          System.out.println("Start to submit file " + file.getName());
              fileSystem.copyFromLocalFile(false, true, new Path(file.getAbsolutePath()), new Path(System.getenv(target)));
          System.out.println("File " + file.getName() + "Submitted");
//          } catch (Exception e) {
//          }
//          list(System.getenv(target));
        }
      } else {
      System.err.println("File or directory doesn't exist");
      }
  }

  private static void list(String dir) throws IOException {
      for (FileStatus fileStatus : fileSystem.listStatus(new Path(dir))) {
          System.out.println(fileStatus.getPath());
      }
  }

  private static void read() {
      try {
      System.out.println("Start to read from HDFS");
//      traverseRead(new Path(System.getenv("remote result")), "");
          traverseRead(new Path("/LE/Result"), "");
      System.out.println("Result pulled from HDFS");
          promptSucceed();
      } catch (IOException exception) {
          promptFailed(exception);
      }
  }

  private static void traverseRead(Path file, String parent) throws IOException {
    System.out.println("Started to read " + file.getName());
      if (fileSystem.isDirectory(file)) {
          for (FileStatus subFile : fileSystem.listStatus(file)) {
              traverseRead(subFile.getPath(), parent + "/" + file.getName());
          }
      } else if (fileSystem.isFile(file)) {
          fileSystem.copyToLocalFile(false, file, new Path(System.getenv("local result") + parent));
      } else {
      System.err.println("The <remote result> doesn't exist. Please check your <remote result> configuration.");
      }
    System.out.println(file.getName() + " read completed");
  }

  private static void trigger() throws IOException {
    System.out.println("Start to trigger dag");
    triggerPost();
    System.out.println("Dag triggered");
    System.out.println("The analysis result will be generated in around 20 minutes.");
  }

  private static void promptSucceed() {
    System.out.println("Operation succeeded");
  }

  private static void promptFailed(Exception e) {
    System.out.println("Operation failed");
    e.printStackTrace();
  }

  private static void triggerPost() throws IOException {
      JsonObject obj = new JsonObject();

      HttpClient httpClient = new DefaultHttpClient();
      String user = "admin";
      String pwd = "admin";
      String encoding = Base64.getEncoder().encodeToString((user + ":" + pwd).getBytes());
      HttpPost http = new HttpPost("http://" + System.getenv("airflow host") + ":8080/api/v1/dags/Analyze/dagRuns");
      http.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
      http.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + encoding);

    http.setEntity(new StringEntity(obj.toString()));
    System.out.println(Arrays.stream(http.getAllHeaders()).map(o->o.toString()).collect(Collectors.joining("\n")));
      HttpResponse response = httpClient.execute(http);
      HttpEntity entity = response.getEntity();
      System.out.println(new BufferedReader(new InputStreamReader(entity.getContent())).lines().collect(Collectors.joining("\n")));
      }
  }

