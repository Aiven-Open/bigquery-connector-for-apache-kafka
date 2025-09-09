package io.aiven.kafka.config.tools;

import io.aiven.kafka.utils.ExtendedConfigKey;
import org.apache.kafka.common.config.ConfigDef;


/**
* Defines the variables that are available for the Velocity template to access a {@link ConfigDef.ConfigKey} object.
   *
       * @see <a href="https://velocity.apache.org/">Apache Velocity</a>
    */
public class ExtendedConfigKeyBean extends ConfigKeyBean {
  /** The key */
  private final boolean extendedFlag;

  /**
   * Constructor.
   *
   * @param key
   *      the Key to wrap.
   */
  public ExtendedConfigKeyBean(final ConfigDef.ConfigKey key) {
    super(key);
    this.extendedFlag = (key instanceof ExtendedConfigKey);
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

  @SuppressWarnings("unused")
  public final boolean isDeprecated() {
    return extendedFlag && asExtended().isDeprecated();
  }

  public final ExtendedConfigKey.DeprecatedInfo deprecated() {
    return extendedFlag ? asExtended().deprecated : null;
  }

}
