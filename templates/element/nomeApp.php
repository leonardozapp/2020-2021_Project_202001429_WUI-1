<?php
if (!isset($showColor)) {
	$showColor = true;
}
if (!isset($showVersion)) {
	$showVersion = false;
}
?>
	<span <?php if ($showColor) {echo 'style ="color:#000000"';} ?>>Fundos</span><span <?php if ($showColor) {echo 'style ="color:#ff0000;"';} ?>>Invest</span>
	<?php if ($showVersion) : ?>
		<span <?php if ($showColor) {
			echo 'style ="color:#000000;"';
		} ?> >
			<small><?php echo$this->element('versionApp'); ?></small>
		</span>
	<?php endif; ?>
