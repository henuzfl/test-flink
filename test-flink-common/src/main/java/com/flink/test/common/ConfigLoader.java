package com.flink.test.common;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.config.ConfigService;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Flink 配置文件加载工具类
 * 支持从以下来源加载配置（优先级从高到低）：
 * 1. 命令行参数 (args)
 * 2. Nacos 配置中心
 * 3. 外部指定配置文件 (--config 路径)
 * 4. Classpath 下的默认资源文件 (如 application.yml)
 */
public class ConfigLoader {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigLoader.class);

    // Nacos 相关参数常量
    private static final String NACOS_SERVER_ADDR = "nacos.server-addr";
    private static final String NACOS_DATA_ID = "nacos.data-id";
    private static final String NACOS_GROUP = "nacos.group";
    private static final String NACOS_NAMESPACE = "nacos.namespace";
    private static final String NACOS_USERNAME = "nacos.username";
    private static final String NACOS_PASSWORD = "nacos.password";
    private static final String NACOS_TIMEOUT = "nacos.timeout";
    private static final String NACOS_FILE_EXT = "nacos.file-extension";

    /**
     * 构建配置参数工具类
     *
     * @param args         命令行参数
     * @param resourceName 默认资源包中的配置文件名
     * @return ParameterTool 实例
     */
    public static ParameterTool loadConfig(String[] args, String resourceName) {
        // 第一步：解析命令行参数
        ParameterTool cliArgs = ParameterTool.fromArgs(args);

        // 第二步：加载 Classpath 下的默认资源（优先级最低）
        ParameterTool config = loadLocalResource(resourceName);

        // 第三步：尝试加载外部配置文件（通过 --config 路径指定）
        String configPath = cliArgs.get("config");
        if (configPath != null) {
            config = loadExternalFile(configPath).mergeWith(config);
        }

        // 第四步：从 Nacos 加载配置（如果配置了相关参数）
        // 这里合并 CLI 参数以便获取 Nacos 连接信息
        ParameterTool bootstrap = config.mergeWith(cliArgs);
        if (bootstrap.has(NACOS_SERVER_ADDR) && bootstrap.has(NACOS_DATA_ID)) {
            config = loadFromNacos(bootstrap).mergeWith(config);
        }

        // 第五步：最后合并命令行参数（优先级最高）
        return config.mergeWith(cliArgs);
    }

    /**
     * 加载 Classpath 下的资源文件
     */
    private static ParameterTool loadLocalResource(String resourceName) {
        try (InputStream is = ConfigLoader.class.getClassLoader().getResourceAsStream(resourceName)) {
            if (is == null) {
                LOG.warn("未在 Classpath 下找到资源文件: {}", resourceName);
                return ParameterTool.fromMap(new HashMap<>());
            }
            return isYaml(resourceName) ? loadYaml(is) : ParameterTool.fromPropertiesFile(is);
        } catch (IOException e) {
            LOG.error("加载本地资源文件失败: {}", resourceName, e);
            return ParameterTool.fromMap(new HashMap<>());
        }
    }

    /**
     * 加载外部指定路径的文件
     */
    private static ParameterTool loadExternalFile(String path) {
        try (InputStream is = new FileInputStream(path)) {
            return isYaml(path) ? loadYaml(is) : ParameterTool.fromPropertiesFile(is);
        } catch (IOException e) {
            LOG.error("加载外部配置文件失败: {}", path, e);
            return ParameterTool.fromMap(new HashMap<>());
        }
    }

    /**
     * 从 Nacos 配置中心拉取配置
     */
    private static ParameterTool loadFromNacos(ParameterTool bootstrap) {
        try {
            String serverAddr = bootstrap.get(NACOS_SERVER_ADDR);
            String dataId = bootstrap.get(NACOS_DATA_ID);
            String group = bootstrap.get(NACOS_GROUP, "DEFAULT_GROUP");
            String namespace = bootstrap.get(NACOS_NAMESPACE, "");
            String username = bootstrap.get(NACOS_USERNAME);
            String password = bootstrap.get(NACOS_PASSWORD);
            long timeout = bootstrap.getLong(NACOS_TIMEOUT, 5000L);

            Properties props = new Properties();
            props.put("serverAddr", serverAddr);
            if (!namespace.isEmpty()) props.put("namespace", namespace);
            if (username != null) props.put("username", username);
            if (password != null) props.put("password", password);

            LOG.info("正在从 Nacos 加载配置: server={}, dataId={}, group={}", serverAddr, dataId, group);
            ConfigService configService = NacosFactory.createConfigService(props);
            String content = configService.getConfig(dataId, group, timeout);

            if (content == null || content.isEmpty()) {
                LOG.warn("Nacos 配置内容为空, dataId: {}", dataId);
                return ParameterTool.fromMap(new HashMap<>());
            }

            // 根据文件后缀或内容特征判定格式
            String fileExt = bootstrap.get(NACOS_FILE_EXT, "properties");
            if ("yml".equalsIgnoreCase(fileExt) || "yaml".equalsIgnoreCase(fileExt) || content.contains(":")) {
                return loadYaml(new ByteArrayInputStream(content.getBytes()));
            } else {
                return ParameterTool.fromPropertiesFile(new ByteArrayInputStream(content.getBytes()));
            }
        } catch (Exception e) {
            LOG.error("从 Nacos 加载配置过程中发生错误", e);
            return ParameterTool.fromMap(new HashMap<>());
        }
    }

    /**
     * 解析并扁平化 YAML 内容
     */
    private static ParameterTool loadYaml(InputStream is) {
        Yaml yaml = new Yaml();
        Map<String, Object> yamlMap = yaml.load(is);
        Map<String, String> flatMap = new HashMap<>();
        flatten("", yamlMap, flatMap);
        return ParameterTool.fromMap(flatMap);
    }

    /**
     * 递归处理 YAML 层级结构，转换为点分隔的 Key (例如 kafka.bootstrap.servers)
     */
    @SuppressWarnings("unchecked")
    private static void flatten(String prefix, Map<String, Object> source, Map<String, String> target) {
        if (source == null) return;
        source.forEach((key, value) -> {
            String fullKey = prefix.isEmpty() ? key : prefix + "." + key;
            if (value instanceof Map) {
                flatten(fullKey, (Map<String, Object>) value, target);
            } else if (value != null) {
                target.put(fullKey, value.toString());
            }
        });
    }

    /**
     * 判断是否为 YAML 格式
     */
    private static boolean isYaml(String fileName) {
        return fileName.endsWith(".yml") || fileName.endsWith(".yaml");
    }
}

