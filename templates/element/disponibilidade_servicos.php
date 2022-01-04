<?php

use Cake\Cache\Cache;
use Cake\Core\Configure;
use Cake\Core\Plugin;
use Cake\Datasource\ConnectionManager;
use Cake\Error\Debugger;
use Cake\Http\Exception\NotFoundException;
?>

<div class="row">
	<div class="column">
		<h4>Disponibilidade dos serviços</h4>
		<?php
		try {
			$connection = ConnectionManager::get('default');
			$connected = $connection->connect();
		} catch (Exception $connectionError) {
			$connected = false;
			$errorMsg = $connectionError->getMessage();
			if (method_exists($connectionError, 'getAttributes')) :
				$attributes = $connectionError->getAttributes();
				if (isset($errorMsg['message'])) :
					$errorMsg .= '<br />' . $attributes['message'];
				endif;
			endif;
		}
		?>
		<ul>
			<?php if ($connected) : ?>
				<li class="bullet success">Acesso à base de dados ok.</li>
			<?php else : ?>
				<li class="bullet problem">Sem acesso à base de dados.<br /><?php echo $errorMsg ?></li>
			<?php endif; ?>
			<?php if (Plugin::isLoaded('DebugKit')) : ?>
				<li class="bullet success">DebugKit ok.</li>
			<?php else : ?>
				<li class="bullet problem">Sem acesso ao debugKite.</li>
			<?php endif; ?>
			<?php if (version_compare(PHP_VERSION, '7.2.0', '>=')) : ?>
				<li class="bullet success">Your version of PHP is 7.2.0 or higher (detected <?php echo PHP_VERSION ?>).</li>
			<?php else : ?>
				<li class="bullet problem">Your version of PHP is too low. You need PHP 7.2.0 or higher to use CakePHP (detected <?php echo PHP_VERSION ?>).</li>
			<?php endif; ?>

			<?php if (extension_loaded('mbstring')) : ?>
				<li class="bullet success">Your version of PHP has the mbstring extension loaded.</li>
			<?php else : ?>
				<li class="bullet problem">Your version of PHP does NOT have the mbstring extension loaded.</li>
			<?php endif; ?>

			<?php if (extension_loaded('openssl')) : ?>
				<li class="bullet success">Your version of PHP has the openssl extension loaded.</li>
			<?php elseif (extension_loaded('mcrypt')) : ?>
				<li class="bullet success">Your version of PHP has the mcrypt extension loaded.</li>
			<?php else : ?>
				<li class="bullet problem">Your version of PHP does NOT have the openssl or mcrypt extension loaded.</li>
			<?php endif; ?>

			<?php if (extension_loaded('intl')) : ?>
				<li class="bullet success">Your version of PHP has the intl extension loaded.</li>
			<?php else : ?>
				<li class="bullet problem">Your version of PHP does NOT have the intl extension loaded.</li>
			<?php endif; ?>
			<?php if (is_writable(TMP)) : ?>
				<li class="bullet success">Your tmp directory is writable.</li>
			<?php else : ?>
				<li class="bullet problem">Your tmp directory is NOT writable.</li>
			<?php endif; ?>

			<?php if (is_writable(LOGS)) : ?>
				<li class="bullet success">Your logs directory is writable.</li>
			<?php else : ?>
				<li class="bullet problem">Your logs directory is NOT writable.</li>
			<?php endif; ?>

			<?php $settings = Cache::getConfig('_cake_core_'); ?>
			<?php if (!empty($settings)) : ?>
				<li class="bullet success">The <em><?php echo $settings['className'] ?> engine</em> is being used for core caching. To change the config edit config/app.php</li>
			<?php else : ?>
				<li class="bullet problem">Your cache is NOT working. Please check the settings in config/app.php</li>
				<?php endif; ?>
				<?php
				if (extension_loaded('apc') && ini_get('apc.enabled')) {
					echo "APC enabled!";
				}
				if (extension_loaded('apcu') && ini_get('apcu.enabled')) {
					echo "APCU enabled!";
				}
				?>
		</ul>
<?php Debugger::checkSecurityKeys(); ?>
	</div>
</div>