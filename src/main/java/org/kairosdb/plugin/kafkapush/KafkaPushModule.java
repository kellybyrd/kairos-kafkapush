package org.kairosdb.plugin.kafkapush;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaPushModule extends AbstractModule
{
	public static final Logger logger = LoggerFactory.getLogger(KafkaPushModule.class);

	@Override
	protected void configure()
	{
		logger.info("Configuring module KafkaPushModule");
		bind(KafkaPushDataPointListener.class).in(Singleton.class);
	}
}
