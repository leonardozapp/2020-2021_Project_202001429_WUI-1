<?php
if (!isset($info)) {
	$info = '';
}
?>

<?php if ($info != '') { ?>
	<div class="info">
		<img src="/img/cake.icon.png">
		<div class="info-content">
			<p><?= $info ?></p>
		</div>
	</div> 
<?php } ?>
