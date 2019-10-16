package me.myds.liuyq.tail;

import static org.fusesource.jansi.Ansi.ansi;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.stream.Stream;

import org.fusesource.jansi.AnsiConsole;

import com.vivimice.bgzfrandreader.RandomAccessBgzFile;

/**
 * tail function
 * 
 * @author liuyq
 *
 */
public class TailGz {
  static {
    AnsiConsole.systemInstall();
  }

  /** line.separator **/
  private static final String BR = System.getProperty("line.separator");
  // private static final String BR = "\n";

  /** blocking queue **/
  private static BlockingQueue<String> processQueue = new LinkedBlockingQueue<>();

  /** log instance **/
  private static final Logger LOGGER = Logger.getLogger(TailGz.class.getName());

  /** DEFAULT_BYTE_SIZE_OFFSET **/
  private static final int DEFAULT_BYTE_SIZE_OFFSET = 500;
  /** DEFAULT_BYTE_SIZE_OFFSET **/
  private static final String DEFAULT_CHARSET = "UTF-8";
  /** DEFAULT_TAIL_LINES **/
  private static final String DEFAULT_TAIL_LINES = "10";

  public static final String ANSI_BLACK_BACKGROUND = "\u001B[40m";
  public static final String ANSI_RED_BACKGROUND = "\u001B[41m";
  public static final String ANSI_GREEN_BACKGROUND = "\u001B[42m";
  public static final String ANSI_YELLOW_BACKGROUND = "\u001B[43m";
  public static final String ANSI_BLUE_BACKGROUND = "\u001B[44m";
  public static final String ANSI_PURPLE_BACKGROUND = "\u001B[45m";
  public static final String ANSI_CYAN_BACKGROUND = "\u001B[46m";
  public static final String ANSI_WHITE_BACKGROUND = "\u001B[47m";

  public static final String ANSI_RESET = "\u001B[0m";
  public static final String ANSI_BLACK = "\u001B[30m";
  public static final String ANSI_RED = "\u001B[31m";
  public static final String ANSI_GREEN = "\u001B[32m";
  public static final String ANSI_YELLOW = "\u001B[33m";
  public static final String ANSI_BLUE = "\u001B[34m";
  public static final String ANSI_PURPLE = "\u001B[35m";
  public static final String ANSI_CYAN = "\u001B[36m";
  public static final String ANSI_WHITE = "\u001B[37m";

  /** line number **/
  private static volatile AtomicLong INDEX = new AtomicLong(1);
  /** os name **/
  private static final String OS_NAME = System.getProperty("os.name");
  /** watchService instance **/
  private static WatchService watchService = null;

  /**
   * main
   * 
   * @param args
   * @throws FileNotFoundException
   */
  public static void main(String[] args) throws FileNotFoundException {

    args = new String[] {"-f", "d:\\bb.gz", "utf-8"};

    if (args.length > 3 || args.length < 2) {
      println("usage: tail -100f [filePath] [charset].", Color.YELLOW_BOLD, true);
      return;
    }

    String arg1 = args[0];
    String arg2 = args[1];
    String arg3 = args.length > 2 ? args[2] : "";


    // RandomAccessBgzFile

    String lineStr = arg1.substring(arg1.indexOf("-") + 1, arg1.indexOf("f"));
    if (lineStr == null || lineStr.isEmpty()) {
      lineStr = DEFAULT_TAIL_LINES;
    }
    String charset = (arg3 == null || arg3.isEmpty()) ? DEFAULT_CHARSET : arg3;

    Integer line = Integer.valueOf(lineStr);
    RandomAccessBgzFile randomAccessFile = null;
    Thread t = null;
    Thread wt = null;
    try {
      File file = new File(arg2);
      randomAccessFile = new RandomAccessBgzFile(new File(arg2));
      addShutdownHook(randomAccessFile, t, wt);
      tryTail(randomAccessFile, line, DEFAULT_BYTE_SIZE_OFFSET, charset);
      randomAccessFile.seek(randomAccessFile.inputLength());

      final RandomAccessBgzFile cloneRandomAccessFile = randomAccessFile;
      wt = new Thread(() -> {
        watchFile(file, cloneRandomAccessFile, charset);
      });
      wt.setName("WatchFile-Thread");
      wt.setDaemon(true);
      wt.start();

      t = new Thread(() -> {
        try {
          listening();
        } catch (InterruptedException e) {
          LOGGER.log(Level.SEVERE, "InterruptedException occurred.", e);
        } catch (Exception e) {
          LOGGER.log(Level.SEVERE, "Exception occurred.", e);
        }
      });
      t.setName("Listening-Thread");
      t.setDaemon(true);
      t.start();
      t.join();
    } catch (Exception e) {
      LOGGER.log(Level.SEVERE, "Exception occurred.", e);
    } finally {
      safetyClose(randomAccessFile);
    }
    AnsiConsole.systemUninstall();
  }

  /**
   * interrupt the thread
   * 
   * @param t
   * @param wt
   */
  private static void interruptT(Thread t, Thread wt) {
    if (t != null) {
      t.interrupt();
    }

    if (wt != null) {
      wt.interrupt();
    }
  }

  /**
   * 安全关闭
   * 
   * @param randomAccessFile
   * @param fileChannel
   */
  private static void safetyClose(RandomAccessBgzFile randomAccessFile) {
    System.out.println();
    if (watchService != null) {
      try {
        watchService.close();
      } catch (IOException e) {}
    }

    if (randomAccessFile != null) {
      try {
        randomAccessFile.close();
        randomAccessFile = null;
      } catch (IOException e) {}
    }
  }

  /**
   * 加入关闭钩子
   * 
   * @param randomAccessFile
   * @param fileChannel
   */
  private static void addShutdownHook(final RandomAccessBgzFile randomAccessFile, final Thread t,
      final Thread wt) {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        try {
          // some cleaning up code...
          interruptT(t, wt);
          processQueue.put("");
          safetyClose(randomAccessFile);
        } catch (InterruptedException e) {}
      }
    });
  }

  /**
   * 监听 blocking queue
   * 
   * @throws InterruptedException
   */
  private static void listening() throws InterruptedException {
    while (!Thread.currentThread().isInterrupted()) {
      String result = processQueue.take();
      if (result == null || result.isEmpty()) {
        continue;
      }

      if (result.startsWith(BR)) {
        System.out.print(BR);
        println(INDEX.getAndIncrement() + "  ", Color.YELLOW, false);
      }

      String[] array = result.split(BR);
      Stream.of(array).parallel().forEachOrdered(l -> {
        if (l == null || l.isEmpty()) {
          System.out.print(BR);
          return;
        }

        if (array.length <= 1) {
          System.out.print(l);
        } else {
          System.out.print(BR);
          println(INDEX.getAndIncrement() + "  ", Color.YELLOW, false);
          System.out.print(l);
        }
      });

      if (!result.equals(BR) && result.endsWith(BR)) {
        System.out.print(BR);
        println(INDEX.getAndIncrement() + "  ", Color.YELLOW, false);
      }
    }
  }

  /**
   * 
   * @param file
   * @param fileChannel
   * @param maxLine
   * @throws IOException
   */
  private static void tryTail(RandomAccessBgzFile file, int maxLine, int offset, String charset)
      throws IOException {
    file.seek((file.inputLength() - offset <= 0) ? 0 : file.inputLength() - offset);

    long start = System.currentTimeMillis();
    String result = fileChannelToString(file, charset);
    long end = System.currentTimeMillis();
    System.out.println("cost " + (end - start) + "ms");

    String[] array = result.split(BR);
    if (array.length >= maxLine || (file.inputLength() - offset) <= 0) {
      Stream.of(array).parallel().skip(array.length - maxLine < 0 ? 0 : array.length - maxLine)
          .forEachOrdered(line -> {
            if (line == null || line.isEmpty()) {
              System.out.print(BR);
              return;
            }
            System.out.print(BR);
            println(INDEX.getAndIncrement() + "  ", Color.YELLOW, false);
            System.out.print(line);
          });

      if (!result.equals(BR) && result.endsWith(BR)) {
        System.out.print(BR);
        println(INDEX.getAndIncrement() + "  ", Color.YELLOW, false);
      }
      return;
    } else {
      tryTail(file, maxLine, offset << 2, charset);
    }
  }

  /**
   * 监视文件所在的文件夹
   * 
   * @param file
   * @param fileChannel
   * @param charset
   */
  private static void watchFile(File file, final RandomAccessBgzFile fileChannel, String charset) {
    Path path = file.toPath().getParent();
    if (path == null) {
      path = new File("./" + file.getName()).toPath().getParent();
    }
    try {
      watchService = FileSystems.getDefault().newWatchService();
      path.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
      while (!Thread.currentThread().isInterrupted()) {
        final WatchKey wk = watchService.take();
        for (WatchEvent<?> event : wk.pollEvents()) {
          // we only register "ENTRY_MODIFY" so the context is always a Path.
          final Path changed = (Path) event.context();
          // System.out.println(changed);
          if (changed.endsWith(file.getName())) {
            final String result = fileChannelToString(fileChannel, charset);
            if (result != null && !result.isEmpty()) {
              processQueue.add(result);
            }
          }
        }
        // reset the key
        boolean valid = wk.reset();
        if (!valid) {
          // System.out.println("Key has been unregisterede");
        }
      }
    } catch (Exception e) {}
  }

  // private Linked

  /**
   * 
   * @param fileChannel
   * @return
   * @throws IOException
   */
  private static String fileChannelToString(RandomAccessBgzFile file, String charset)
      throws IOException {
    StringBuffer result = new StringBuffer();
    byte[] bytes = new byte[1024];
    while (file.read(bytes) > 0) {
      result.append(new String(bytes, charset));
    }
    return result.toString();

  }

  /**
   * 
   * @param line
   */
  private static void println(String context, Color color, boolean line) {
    if (context == null || context.isEmpty()) {
      return;
    }

    if (OS_NAME.equalsIgnoreCase("linux")) {
      if (line) {
        System.out
            .println(String.format("%s%s%s", color.getCode(), context, Color.RESET.getCode()));
      } else {
        System.out.print(String.format("%s%s%s", color.getCode(), context, Color.RESET.getCode()));
      }
    } else {
      org.fusesource.jansi.Ansi.Color c = null;
      switch (color) {
        case BLUE:
          c = org.fusesource.jansi.Ansi.Color.BLUE;
          break;
        case RED:
          c = org.fusesource.jansi.Ansi.Color.RED;
          break;
        case GREEN:
          c = org.fusesource.jansi.Ansi.Color.GREEN;
        case YELLOW:
        case YELLOW_BOLD:
          c = org.fusesource.jansi.Ansi.Color.YELLOW;
          break;
        default:
          c = org.fusesource.jansi.Ansi.Color.WHITE;
          break;
      }

      if (line) {
        System.out.println(ansi().fg(c).a(String.format("%s", context)).reset());
      } else {
        System.out.print(ansi().fg(c).a(String.format("%s", context)).reset());
      }
    }
  }

  /**
   * prefixLines
   * 
   * @param prefix
   * @param input
   * @return
   */
  public static String prefixLines(String prefix, String input) {
    return input.replaceAll(".*\\R|.+\\z", Matcher.quoteReplacement(prefix) + "$0");
  }

  static enum Color {
    // 颜色结尾字符串，重置颜色的
    RESET("\033[0m"),

    // Regular Colors 普通颜色，不带加粗，背景色等
    BLACK("\033[0;30m"), // BLACK
    RED("\033[0;31m"), // RED
    GREEN("\033[0;32m"), // GREEN
    YELLOW("\033[0;33m"), // YELLOW
    BLUE("\033[0;34m"), // BLUE
    MAGENTA("\033[0;35m"), // MAGENTA
    CYAN("\033[0;36m"), // CYAN
    WHITE("\033[0;37m"), // WHITE

    // Bold
    BLACK_BOLD("\033[1;30m"), // BLACK
    RED_BOLD("\033[1;31m"), // RED
    GREEN_BOLD("\033[1;32m"), // GREEN
    YELLOW_BOLD("\033[1;33m"), // YELLOW
    BLUE_BOLD("\033[1;34m"), // BLUE
    MAGENTA_BOLD("\033[1;35m"), // MAGENTA
    CYAN_BOLD("\033[1;36m"), // CYAN
    WHITE_BOLD("\033[1;37m"), // WHITE

    // Underline
    BLACK_UNDERLINED("\033[4;30m"), // BLACK
    RED_UNDERLINED("\033[4;31m"), // RED
    GREEN_UNDERLINED("\033[4;32m"), // GREEN
    YELLOW_UNDERLINED("\033[4;33m"), // YELLOW
    BLUE_UNDERLINED("\033[4;34m"), // BLUE
    MAGENTA_UNDERLINED("\033[4;35m"), // MAGENTA
    CYAN_UNDERLINED("\033[4;36m"), // CYAN
    WHITE_UNDERLINED("\033[4;37m"), // WHITE

    // Background
    BLACK_BACKGROUND("\033[40m"), // BLACK
    RED_BACKGROUND("\033[41m"), // RED
    GREEN_BACKGROUND("\033[42m"), // GREEN
    YELLOW_BACKGROUND("\033[43m"), // YELLOW
    BLUE_BACKGROUND("\033[44m"), // BLUE
    MAGENTA_BACKGROUND("\033[45m"), // MAGENTA
    CYAN_BACKGROUND("\033[46m"), // CYAN
    WHITE_BACKGROUND("\033[47m"), // WHITE

    // High Intensity
    BLACK_BRIGHT("\033[0;90m"), // BLACK
    RED_BRIGHT("\033[0;91m"), // RED
    GREEN_BRIGHT("\033[0;92m"), // GREEN
    YELLOW_BRIGHT("\033[0;93m"), // YELLOW
    BLUE_BRIGHT("\033[0;94m"), // BLUE
    MAGENTA_BRIGHT("\033[0;95m"), // MAGENTA
    CYAN_BRIGHT("\033[0;96m"), // CYAN
    WHITE_BRIGHT("\033[0;97m"), // WHITE

    // Bold High Intensity
    BLACK_BOLD_BRIGHT("\033[1;90m"), // BLACK
    RED_BOLD_BRIGHT("\033[1;91m"), // RED
    GREEN_BOLD_BRIGHT("\033[1;92m"), // GREEN
    YELLOW_BOLD_BRIGHT("\033[1;93m"), // YELLOW
    BLUE_BOLD_BRIGHT("\033[1;94m"), // BLUE
    MAGENTA_BOLD_BRIGHT("\033[1;95m"), // MAGENTA
    CYAN_BOLD_BRIGHT("\033[1;96m"), // CYAN
    WHITE_BOLD_BRIGHT("\033[1;97m"), // WHITE

    // High Intensity backgrounds
    BLACK_BACKGROUND_BRIGHT("\033[0;100m"), // BLACK
    RED_BACKGROUND_BRIGHT("\033[0;101m"), // RED
    GREEN_BACKGROUND_BRIGHT("\033[0;102m"), // GREEN
    YELLOW_BACKGROUND_BRIGHT("\033[0;103m"), // YELLOW
    BLUE_BACKGROUND_BRIGHT("\033[0;104m"), // BLUE
    MAGENTA_BACKGROUND_BRIGHT("\033[0;105m"), // MAGENTA
    CYAN_BACKGROUND_BRIGHT("\033[0;106m"), // CYAN
    WHITE_BACKGROUND_BRIGHT("\033[0;107m"); // WHITE

    private final String code;

    public String getCode() {
      return code;
    }

    Color(String code) {
      this.code = code;
    }

    @Override
    public String toString() {
      return code;
    }
  }

}
