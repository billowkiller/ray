package org.ray.runtime.functionmanager;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.ray.api.annotation.RayRemote;
import org.ray.api.function.RayFunc0;
import org.ray.api.function.RayFunc1;
import org.ray.api.id.UniqueId;
import org.ray.runtime.functionmanager.FunctionManager.DriverFunctionTable;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests for {@link FunctionManager}
 */
public class FunctionManagerTest {

  @RayRemote
  public static Object foo() {
    return null;
  }

  @RayRemote
  public static class Bar {

    public Bar() {
    }

    public Object bar() {
      return null;
    }
  }

  private static RayFunc0<Object> fooFunc;
  private static RayFunc1<Bar, Object> barFunc;
  private static RayFunc0<Bar> barConstructor;
  private static FunctionDescriptor fooDescriptor;
  private static FunctionDescriptor barDescriptor;
  private static FunctionDescriptor barConstructorDescriptor;

  @BeforeClass
  public static void beforeClass() {
    fooFunc = FunctionManagerTest::foo;
    barConstructor = Bar::new;
    barFunc = Bar::bar;
    fooDescriptor = new FunctionDescriptor(FunctionManagerTest.class.getName(), "foo",
        "()Ljava/lang/Object;");
    barDescriptor = new FunctionDescriptor(Bar.class.getName(), "bar",
        "()Ljava/lang/Object;");
    barConstructorDescriptor = new FunctionDescriptor(Bar.class.getName(),
        FunctionManager.CONSTRUCTOR_NAME,
        "()V");
  }

  @Test
  public void testGetFunctionFromRayFunc() {
    final FunctionManager functionManager = new FunctionManager(null);
    // Test normal function.
    RayFunction func = functionManager.getFunction(UniqueId.NIL, fooFunc);
    Assert.assertFalse(func.isConstructor());
    Assert.assertEquals(func.getFunctionDescriptor(), fooDescriptor);
    Assert.assertNotNull(func.getRayRemoteAnnotation());

    // Test actor method
    func = functionManager.getFunction(UniqueId.NIL, barFunc);
    Assert.assertFalse(func.isConstructor());
    Assert.assertEquals(func.getFunctionDescriptor(), barDescriptor);
    Assert.assertNull(func.getRayRemoteAnnotation());

    // Test actor constructor
    func = functionManager.getFunction(UniqueId.NIL, barConstructor);
    Assert.assertTrue(func.isConstructor());
    Assert.assertEquals(func.getFunctionDescriptor(), barConstructorDescriptor);
    Assert.assertNotNull(func.getRayRemoteAnnotation());
  }

  @Test
  public void testGetFunctionFromFunctionDescriptor() {
    final FunctionManager functionManager = new FunctionManager(null);
    // Test normal function.
    RayFunction func = functionManager.getFunction(UniqueId.NIL, fooDescriptor);
    Assert.assertFalse(func.isConstructor());
    Assert.assertEquals(func.getFunctionDescriptor(), fooDescriptor);
    Assert.assertNotNull(func.getRayRemoteAnnotation());

    // Test actor method
    func = functionManager.getFunction(UniqueId.NIL, barDescriptor);
    Assert.assertFalse(func.isConstructor());
    Assert.assertEquals(func.getFunctionDescriptor(), barDescriptor);
    Assert.assertNull(func.getRayRemoteAnnotation());

    // Test actor constructor
    func = functionManager.getFunction(UniqueId.NIL, barConstructorDescriptor);
    Assert.assertTrue(func.isConstructor());
    Assert.assertEquals(func.getFunctionDescriptor(), barConstructorDescriptor);
    Assert.assertNotNull(func.getRayRemoteAnnotation());
  }

  @Test
  public void testLoadFunctionTableForClass() {
    DriverFunctionTable functionTable = new DriverFunctionTable(getClass().getClassLoader());
    Map<Pair<String, String>, RayFunction> res = functionTable
        .loadFunctionsForClass(Bar.class.getName());
    // The result should 2 entries, one for the constructor, the other for bar.
    Assert.assertEquals(res.size(), 2);
    Assert.assertTrue(res.containsKey(
        ImmutablePair.of(barDescriptor.name, barDescriptor.typeDescriptor)));
    Assert.assertTrue(res.containsKey(
        ImmutablePair.of(barConstructorDescriptor.name, barConstructorDescriptor.typeDescriptor)));
  }

  @Test
  public void testGetFunctionFromLocalResource() throws Exception {
    final String resourcePath = "/tmp/ray/java_test/resource";
    UniqueId driverId = UniqueId.randomId();
    final String driverResourcePath = resourcePath + "/" + driverId.toString();
    File driverResourceDir = new File(driverResourcePath);
    driverResourceDir.mkdirs();

    String demoJavaFile = "";
    demoJavaFile += "public class DemoApp {\n";
    demoJavaFile += "  public static String hello() {\n";
    demoJavaFile += "    return \"hello\";\n";
    demoJavaFile += "  }\n";
    demoJavaFile += "}";

    try {
      // Write the demo java file to the driver resource path.
      String javaFilePath = driverResourcePath + "/DemoApp.java";
      Files.write(Paths.get(javaFilePath), demoJavaFile.getBytes());

      // Compile the java file.
      JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
      int result = compiler.run(null, null, null, "-d", driverResourcePath, javaFilePath);
      if (result != 0) {
        throw new RuntimeException("Couldn't compile Demo.java.");
      }
      // Package the class file into a jar file.
      String[] packJarCommand = new String[]{
          "jar",
          "-cvf",
          driverResourcePath + "/DemoApp.jar",
          "DemoApp.class"
      };
      new ProcessBuilder(packJarCommand).directory(driverResourceDir).start().waitFor();

      // Test loading the function.
      FunctionDescriptor descriptor = new FunctionDescriptor(
          "DemoApp", "hello", "()Ljava/lang/String;");
      final FunctionManager functionManager = new FunctionManager(resourcePath);
      RayFunction func = functionManager.getFunction(driverId, descriptor);
      Assert.assertEquals(func.getFunctionDescriptor(), descriptor);
    } finally {
      FileUtils.deleteDirectory(driverResourceDir);
    }
  }

}
