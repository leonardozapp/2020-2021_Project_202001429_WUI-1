<script type="text/javascript">
	google.charts.load('current', {'packages': ['corechart']});
	google.charts.setOnLoadCallback(drawChart);

<?php
if (!isset($title)) {
	$title = '';
}
if (!isset($vAxisTitle)) {
	$vAxisTitle = '';
}
if (!isset($hAxisTitle)) {
	$hAxisTitle = 'data';
}
if (!isset($vAxisFormat)) {
	$vAxisFormat = '';
}
if (!isset($hAxisFormat)) {
	$hAxisFormat = 'd/m/y';
}
if (!isset($chartType)) {
	$chartType = 'Area';
}
if (!isset($dataType)) {
	$dataType = 'arrayToDataTable';
}
?>

	function drawChart() {
<?php
echo('var data = google.visualization.');
echo ($dataType . '(');
if ($dataType == 'arrayToDataTable') {
	echo('[');
} else { // $dataType == 'DataTable'
	echo(');');
}
foreach ($data as $datum) {
	echo $datum;
}
if ($dataType == 'arrayToDataTable') {
	echo(']);');
}
?>

		var options = {
			//title: '<= $title ?>',
			//chartArea: {width: '75%', height: '75%'},
			chartArea: {top: 30, left: 100, bottom: 80, width: "100%", height: "100%"},
			is3D: 'true',
			crosshair: {trigger: 'both'},
			legend: {position: 'top'},
			hAxis: {title: '<?= $hAxisTitle ?>', titleTextStyle: {color: '#333'},
				format: '<?= $hAxisFormat ?>', gridlines: {color: '#333', minSpacing: 60}, viewWindowMode: 'pretty'},
			vAxis: {title: '<?= $vAxisTitle ?>', format: '<?= $vAxisFormat ?>', viewWindowMode: 'pretty'}, //, 			areaOpacity: 0.2,
			enableInteractivity: 'true',
			lineWidth: 2,
			aggregationTarget: 'category',
			animation: {easing: 'inAndOut', duration: '2000', startup: 'true'},
			sizeAxis: {minValue: 6, maxSize: 10},
			bubble: {textStyle: {auraColor: 'yellow', fontSize: 10}},
			focusTarget: <?php
if ($chartType == 'Bubble')
	echo("'datum'");
else
	echo("'category'");
?>,
		};

		var chart = new google.visualization.<?= $chartType ?>Chart(document.getElementById('<?= $chart ?>'));
		chart.draw(data, options);
	}
</script>