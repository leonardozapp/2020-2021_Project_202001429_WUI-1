<?php
if (!isset($tam)) {
	$tam = '4';
}
if (!isset($info)) {
	$info = '';
}
if (!isset($align)) {
	$align = 'left';
}
if (isset($h)) {
	switch ($h) {
		case 1: $tam = 7;
			break;
		case 2: $tam = 6;
			break;
		case 3: $tam = 5;
			break;
		default: $tam = 4;
	}
}
?>
<div style="text-align: <?= $align ?>;">
	<?php if (isset($h) && $info=='') { ?>
		<h<?= $h ?>><?= $title ?></h<?= $h ?>>
	<?php } else { ?>
		<font size='<?= $tam ?>'><?= $title ?> </font>
	<?php } ?>
		
	<?=$this->element('info', array('info' => $info)) ?>
</div>


