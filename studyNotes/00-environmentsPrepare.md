# Hadoop环境配置之配置Maven和Eclipse

> 种一棵树最好的时间是十年前，其次就是现在

之前我们讲了如何在虚拟机上运行Hadoop程序，但我们都是在Linux的shell编程中进行的。在真实的开发场景中，我们需要使用Java来操作Hadoop程序，所以我们需要在Windows下配置Java开发环境，来运行Hadoop项目。

- 这里，我们选择的IDE是Eclipse，因为它是一个简单开源易上手的开发工具，在我们学习Hadoop的时候，一门简单的工具对于我们学习知识是很有帮助的。大家如果觉得Eclipse太古老了，也可以安装intellij IDEA，这是一款更为强大的IDE，在之后我们也会涉及到。

- 至于Maven：它是一个用于帮助我们管理Jar包的工具，有了它之后，我们只需要在配置文件中添加好我们所需要的Jar包的信息，它就会自动帮助我们把相应的Jar包下载到我们的项目中去，这大大减少了我们配置项目的时间。

接下来，就让我们进入具体的安装配置过程吧！

所有的文件都已上传到微信公众号：在下图灵。快扫码关注吧

[![GUVPzQ.png](https://s1.ax1x.com/2020/04/03/GUVPzQ.png)](https://imgchr.com/i/GUVPzQ)

## 在Windows上配置Maven

### 安装Mawen

1. 在[Apache](http://maven.apache.org/download.cgi)官网下载Maven的压缩包，然后解压到自己的电脑上

2. 配置环境变量

   - 在环境变量中添加MAVEN_HOME，路径为你的Maven解压路径

   - 在Path中添加%MAVEN_HOME%/bin

   - 在CMD中输入：

     ```bash
     mvn -v
     ```

     看是否有对应的Maven版本信息输出，如果有，那么说明您的Maven安装成功了

### 配置Maven仓库

1. 为什么要配置Maven仓库？

   因为Maven的服务器在国外，由于墙的原因，每次我们下载依赖的时候，需要连接国外的服务器，速度特别慢，所以我们配置国内的阿里云的服务器，这样我们下载速度会大大大大加快。

2. 配置阿里云仓库，配置方法如下

   - 进入到：%MAVEN_HOME%/conf问价夹下
   
   - 打开settings.xml文件
   
   - 查找mirror，在找到的地方（一般是150行附近）添加如下信息，这是aliyun仓库的配置代码：
   
     ```xml
       <mirror>
         <id>aliyun</id>
         <mirrorOf>*</mirrorOf>
         <name>aliyun Maven</name>
         <url>http://maven.aliyun.com/nexus/content/groups/public</url>
       </mirror>
     ```
   
     具体如图所示
   
     [![GUxWgP.png](https://s1.ax1x.com/2020/04/03/GUxWgP.png)](https://imgchr.com/i/GUxWgP)
   
3. 配置本地仓库

   配置本地仓库是为了避免每次我们都从阿里云仓库下载依赖包：比如之前我们下载某个包到本地了，下次使用它的时候，就可以直接在本地调用它，不用再去阿里云仓库下载了，这进一步提升了速度。配置方法如下：

   - 首先在%MAVEN_HOME%目录下，创建一个文件夹为mavenRepository作为本地仓库

   - 打开%MAVEN_HOME%/conf/settings.xml文件，搜索localRepository后（一般在50行附近）在下面添加如下信息：

     ```xml
     <localRepository>E:/Environmens/apache-maven-3.6.3-bin/mavenRepository</localRepository>
     ```

     注意斜杠的方向；路径必须是自己刚才创建的mavenRepository的路径，如下图所示：

     ![GNHcmn.png](https://s1.ax1x.com/2020/04/03/GNHcmn.png)

   - 最后，把conf文件夹中的settings.xml文件，复制一份到我们刚才创建的mavenRepository文件夹中去

   至此，Maven的配置工作已经全部完成，让我们开始在Eclipse中配置Maven吧！



## 在Eclipse中配置Maven

### 安装Eclipse

1. 先去[Eclipse的官网](https://www.eclipse.org/downloads/)下载Eclipse的安装程序，下载最新的版本即可。
2. 将Eclipse安装到自己喜欢的位置即可

### 在Eclipse中配置Maven

之前老版本的Eclipse不自带Maven，需要额外去安装Maven的插件，如今新版本的Eclipse都自带Maven插件了。所以我们直接在Eclipse中配置Maven的配置信息即可。

1. 打开Eclipse新建一个MavenDemo项目，如图所示：

   ![GNbHgg.png](https://s1.ax1x.com/2020/04/03/GNbHgg.png)

2. 点击Windows->Preferences->Maven->Installation->Add，添加Maven的安装目录，如图所示：

   ![GNqe56.png](https://s1.ax1x.com/2020/04/03/GNqe56.png)

   点击finish后，记得勾选上我们新添加的Maven，如图所示：

   [![GNqoZR.png](https://s1.ax1x.com/2020/04/03/GNqoZR.png)](https://imgchr.com/i/GNqoZR)

3. 点击User Settings

   - 在Global Settings中，选择%MAVEN_HOME%/conf/settings.xml文件

   - 在User Settings中，选择%MAVEN_HOMT%/mavenRepository/settings.xml文件

   - 点击update settings并且保存即可，如下图所示：

     [![GNqqJK.png](https://s1.ax1x.com/2020/04/03/GNqqJK.png)](https://imgchr.com/i/GNqqJK)

   至此，在Eclipse中配置maven的完整过程已经结束了，我们可以创建一个项目来测试一下。



## 创建一个Maven项目

### 新建Maven项目

1. 新建一个项目，如图所示：

   ![GNOpX4.png](https://s1.ax1x.com/2020/04/03/GNOpX4.png)

2. 勾选上Create a simple project，如图所示：

   ![GNOi7R.png](https://s1.ax1x.com/2020/04/03/GNOi7R.png)

3. 设置项目的groupID和artifactID，如图所示：

   [![GNOMBd.png](https://s1.ax1x.com/2020/04/03/GNOMBd.png)](https://imgchr.com/i/GNOMBd)

   这样我们就创建了一个Maven项目

### 认识Maven项目目录

一个生成好的Maven项目的目录结构如图所示：

![image-20200403140904483](C:\Users\FuJie\AppData\Roaming\Typora\typora-user-images\image-20200403140904483.png)

下面来介绍各部分的作用：

- src/main/java：这个是存放我们自己编写的Java程序的目录
- src/main/resource：这个存放我们项目所需的一些配置信息
- src/test/java：这里存放我们编写的测试类，用于测试我们编写的Java程序
- src/test/resource：这里存放的是测试类的配置信息
- JRE System Library：这里存放JRE中的jar文件，可以指定为本机上JDK中的JRE
- Maven Dependencies：Maven自动帮我们下载的Jar包，都放在这里
- target：输出的项目Jar文件存放目录
- pom.xml：这个是Maven配置信息

### 创建一个HelloWorld.java文件

1. 创建HelloWorld.java文件

   ![GUCc2n.png](https://s1.ax1x.com/2020/04/03/GUCc2n.png)

2. 写入以下代码：

   ```java
   package com.iamturing;
   
   public class HelloWorld {
   	
   	public void say() {
   		System.out.print("hello world");
   	}
   
   }
   
   ```

   如图所示：

   ![GUPPxI.png](https://s1.ax1x.com/2020/04/03/GUPPxI.png)

### 创建测试文件TestHelloWorld.java

1. 创建一个TestHelloWorld.java文件：

   [![GUEAUK.png](https://s1.ax1x.com/2020/04/03/GUEAUK.png)](https://imgchr.com/i/GUEAUK)

2. 输入以下内容：

   ```java
   package com.iamturing;
   public class TestHelloWorld {
   	public void say() {
   		new HelloWorld().say();
   	}
   }
   ```

3. 为了让我们的程序跑起来，我们需要添加Junit测试模块，在pom.xml中添加以下信息：

   ```xml
     <dependencies>
     	<!-- https://mvnrepository.com/artifact/org.junit.jupiter/junit-jupiter-api -->
   	<dependency>
   	    <groupId>org.junit.jupiter</groupId>
   	    <artifactId>junit-jupiter-api</artifactId>
   	    <version>5.5.2</version>
   	    <scope>test</scope>
   	</dependency>
     	
     </dependencies>
   ```

   保存之后，Maven会自动帮我们下载Junit的Jar包，如图所示：

   ![GUE858.png](https://s1.ax1x.com/2020/04/03/GUE858.png)

4. 返回到TestHelloWorld.java中，添加Junit测试部分

   输入以下内容即可：

   ```java
   package com.iamturing;
   
   import org.junit.jupiter.api.Test;
   
   public class TestHelloWorld {
   	
   	@Test
   	public void say() {
   		new HelloWorld().say();
   	}
   
   }
   ```

   右键运行，控制台会输出hello world。至此，我们Hadoop前期大家Maven环境搭建已经成功。下一节，我将带领大家一起在Eclipse中配置Hadoop的相关依赖，希望大家继续加油！



