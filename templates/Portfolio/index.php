<?php
/**
 */
?>
<div class="Portfolio index content">
	<h2><?= __('Análise e Otimização') ?></h2>
	<div class="row">
		<h4><?= __('	Adicione os fundos que deseja comparar aqui no campo abaixo:') ?></h4>
	</div>
	<div class="row">
		<h3><?= __('Análise do retorno e risco') ?></h3>
	</div>
	<div class="row">
		<div class="column">
			<script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
			<script type="text/javascript">
				google.charts.load('current', {'packages': ['corechart']});
				google.charts.setOnLoadCallback(drawSeriesChart);

				function drawSeriesChart() {

					var data = google.visualization.arrayToDataTable([
					['ID', 'Risco', 'Retorno', 'Categoria', ''],
<?php
$retMax = 0.0;
$devMax = 0.0;
foreach ($retornoRiscoFundos as $retornoRiscoFundo):
	?>
	<?php echo("['F" . $retornoRiscoFundo->cnpj_fundo_id . ": " . $retornoRiscoFundo->cnpj_fundo->DENOM_SOCIAL . "'," . $retornoRiscoFundo->desvio_padrao . "," . $retornoRiscoFundo->rentab_media . ",'" . $retornoRiscoFundo->cnpj_fundo->cadastro_fundos[0]->tipo_classe_fundo->classe . "', 15],"); ?>
	<?php if ($retornoRiscoFundo->rentab_media > $retMax) $retMax = $retornoRiscoFundo->rentab_media; ?>
	<?php if ($retornoRiscoFundo->desvio_padrao > $devMax) $devMax = $retornoRiscoFundo->desvio_padrao; ?>
<?php endforeach; ?>
					]);

					var options = {
						title: 'Risco X Retorno',
						hAxis: {title: 'Risco', viewWindowMode: 'pretty', viewWindow: {max:<?php echo ($devMax * 1.1); ?>}}, //, format: 'percent'},
						vAxis: {title: 'Retorno', viewWindowMode: 'pretty', viewWindow: {max:<?php echo ($retMax * 1.1); ?>}}, //, format: 'percent'},
						sizeAxis: {minValue: 15, maxSize: 20},
						bubble: {
							textStyle: {auraColor: 'none'}
						}
					}

					var chart = new google.visualization.BubbleChart(document.getElementById('chart1_div'));
					chart.draw(data, options);
				}
			</script>
			<div id="chart1_div" style="width: 100%; height: 500px;"></div>
		</div>
	</div>

	<div class="row">
		<h3><?= __('Análise de correlação') ?></h3>
	</div>
	<div class="row">
		<table>
			<thead>
				<tr>
					<th>Fundos</th>
					<?php foreach ($alocacoes['fundos_id'] as $peso): ?>
						<th>F<?= $peso ?></th>
					<?php endforeach; ?>
				</tr>
				<?php $i = 1;
				foreach ($alocacoes['fundos_id'] as $peso): ?>
					<tr>
						<td>
							F<?= $peso ?>
						</td>
					<?php for ($j = 1; $j <= $i; $j++)
						echo('<td></td>'); $i++; ?>
					</tr>
<?php endforeach; ?>
			</thead>
		</table>
	</div>

	<div class="row">
		<h3><?= __('Fronteira eficiente e otimização') ?></h3>
	</div>
	<div class="row">
		<div class="table-responsive">
			<script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
			<script type="text/javascript">
				google.charts.load('current', {'packages': ['corechart']});
				google.charts.setOnLoadCallback(drawSeriesChart);

				function drawSeriesChart() {

					var data = google.visualization.arrayToDataTable([
					['Carteira', 'Risco', 'Retorno', 'Fronteira'], //, 'Population'],
<?php
$retMax = 0.0;
$retMin = 1e3;
$devMax = 0.0;
foreach ($alocacoes['alocacoes'] as $aloc):
	?>
	<?php echo("['" . $aloc['id'] . "'," . $aloc['risco'] . "," . $aloc['rentabilidade'] . ",'" . $aloc['inFronteira'] . "'],"); ?>
	<?php if ($aloc['rentabilidade'] > $retMax) $retMax = $aloc['rentabilidade']; ?>
	<?php if ($aloc['rentabilidade'] < $retMin) $retMin = $aloc['rentabilidade']; ?>
	<?php if ($aloc['risco'] > $devMax) $devMax = $aloc['risco']; ?>
<?php endforeach; ?>
					]);

					var options = {
						title: 'Fronteira Eficiente de Carteiras de Investimento',
						hAxis: {title: 'Risco', viewWindowMode: 'pretty', viewWindow: {max: <?php echo ($devMax * 1.05); ?>}}, //, format: 'percent'},
						vAxis: {title: 'Retorno', viewWindowMode: 'pretty', viewWindow: {min: <?php echo ($retMin * 0.95); ?>, max: <?php echo ($retMax * 1.05); ?>}}, //, format: 'percent'},
						colorAxis: {minValue: 0, colors: ['#000000', '#00FF00']},
						legend: 'none', //{position: 'right', textStyle: {color: 'blue', fontSize: 11}},
						sizeAxis: {minValue: 5, maxSize: 9},
						bubble: {
							textStyle: {auraColor: 'none', fontSize: 10}
						}
					}

					var chart = new google.visualization.BubbleChart(document.getElementById('chart2_div'));
					chart.draw(data, options);
				}
			</script>
			<div id="chart2_div" style="width: 100%; height: 600px;"></div>
		</div>
	</div>
	<div class="row">
		<h4><?= __('Carteiras na Fronteira Eficiente') ?></h4>
	</div>
	<div class="row">
		<table>
			<thead>
				<tr>
					<th>Carteira</th>
					<?php foreach ($alocacoes['fundos_id'] as $peso): ?>
						<th>Peso F<?= $peso ?></th>
<?php endforeach; ?>
					<th>Rentabilidade</th>
					<th>Risco</th>
				</tr>
			</thead>
<?php foreach ($alocacoes['alocacoes'] as $aloc): ?>
						<?php if ($aloc['inFronteira'] > 0) { ?>
					<tr>
						<td>
						<?php echo($aloc['id']); ?>
						</td>
							<?php foreach ($aloc['pesos'] as $peso): ?>
							<td>
							<?php echo($this->Number->toPercentage($peso * 100, 2)); ?>
							</td>
							<?php endforeach; ?>
						<td>
							<?php echo($this->Number->toPercentage($aloc['rentabilidade'], 2)); ?>
						</td>
						<td>
					<?php echo($this->Number->toPercentage($aloc['risco'], 2)); ?>
						</td>
					</tr>
	<?php } ?>
<?php endforeach; ?>
		</table>
	</div>

</div>
