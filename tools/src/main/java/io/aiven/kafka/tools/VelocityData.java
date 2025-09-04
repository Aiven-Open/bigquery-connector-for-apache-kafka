package io.aiven.kafka.tools;

import io.aiven.kafka.utils.ExtendedConfigKey;
import java.util.List;
import org.apache.kafka.common.config.ConfigDef;

/**
* Defines the variables that are available for the Velocity template to access a {@link ConfigDef.ConfigKey} object.
   *
       * @see <a href="https://velocity.apache.org/">Apache Velocity</a>
    */
public class VelocityData {
  /** The key */
  private final ConfigDef.ConfigKey key;
  private final boolean extendedFlag;

  /**
   * Constructor.
   *
   * @param key
   *      the Key to wrap.
   */
  public VelocityData(final ConfigDef.ConfigKey key) {
    this.key = key;
    this.extendedFlag = (key instanceof ExtendedConfigKey);
  }

  /**
   * Gets the name of the key.
   *
   * @return The name of the key.
   */
  public final String getName() {
    return key.name;
  }

  /**
   * Gets the data type of the entry.
   *
   * @return The data type of the entry.
   */
  public final ConfigDef.Type getType() {
    return key.type;
  }

  /**
   * Gets the documentation for the entry.
   *
   * @return The documentation for the entry.
   */
  public final String getDocumentation() {
    return key.documentation;
  }

  /**
   * Gets the default value for the entry.
   *
   * @return The default value for the entry. May be {@code null}.
   */
  public final Object getDefaultValue() {
    return ConfigDef.NO_DEFAULT_VALUE.equals(key.defaultValue) ? null : key.defaultValue;
  }

  /**
   * Gets the validator for the entry.
   *
   * @return The Validator for the entry.
   */
  public final ConfigDef.Validator getValidator() {
    return key.validator;
  }

  /**
   * Gets the importance of the entry.
   *
   * @return the importance of the entry.
   */
  public final ConfigDef.Importance getImportance() {
    return key.importance;
  }

  /**
   * Gets the group of the entry.
   *
   * @return The group of the entry,
   */
  public final String getGroup() {
    return key.group;
  }

  /**
   * Gets the order in the group of the entry.
   *
   * @return the order in the group of the entry.
   */
  public final int getOrderInGroup() {
    return key.orderInGroup;
  }

  /**
   * Gets the width estimate for the entry.
   *
   * @return The width estimate for the entry.
   */
  public final ConfigDef.Width getWidth() {
    return key.width;
  }

  /**
   * Gets the display name for the entry.
   *
   * @return The display name for the entry.
   */
  public final String getDisplayName() {
    return key.displayName;
  }

  /**
   * Gets the list of dependents for the entry.
   *
   * @return The list of dependents for the entry.
   */
  public final List<String> getDependents() {
    return key.dependents;
  }

  /**
   * Gets the recommender for the entry.
   *
   * @return the recommender for the entry.
   */
  public final ConfigDef.Recommender getRecommender() {
    return key.recommender;
  }

  /**
   * Gets the internal config flag.
   *
   * @return The internal config flag.
   */
  public final boolean isInternalConfig() {
    return key.internalConfig;
  }

  public final boolean isExtendedFlag() {
    return extendedFlag;
  }

  private ExtendedConfigKey asExtended() {
    return extendedFlag ? (ExtendedConfigKey) key : null;
  }

  public final String since() {
    return extendedFlag ? asExtended().since : null;
  }

  public final boolean isDeprecated() {
    return extendedFlag && asExtended().isDeprecated();
  }

  public final ExtendedConfigKey.DeprecatedInfo deprecated() {
    return extendedFlag ? asExtended().deprecated : null;
  }

}
