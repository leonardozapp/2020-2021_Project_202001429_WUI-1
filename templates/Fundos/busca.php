<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\CnpjFundo[]|\Cake\Collection\CollectionInterface $cnpjFundos
 */
?>
<!--div class="cnpjFundos index content"-->
<!-- $this->Html->link(__('New Cnpj Fundo'), ['action' => 'add'], ['class' => 'button float-right']) ?> -->
<?= $this->element('titleInfo', array('title' => __('Busca Geral de Fundos de Investimento'), 'h' => 1)); ?>

<div class="column-responsive">
	<?php echo$this->element('titleInfo', array('title' => __('Filtros'), 'h' => 2, 'info' => 'Os filtros são usados para restringir a busca por fundos com características de seu interesse. Preencha um ou mais campos abaixo com as características desejadas e pressione o botão "Aplicar filtros" para refinar sua busca.')); ?>
	<?= $this->Form->create($formFiltro) ?>
	<fieldset>
		<?= $this->Form->control('nome', ['type' => 'text', 'label' => __('Nome do fundo')]); ?>
		<!--?= $this->element('info', array('info' => 'Você pode digitar apenas partes do nome do fundo. Inicialmente serão procurados fundos contendo exatamente o texto digitado, e depois serão procurados fundos que contenham uma ou mais partes do texto.')) ?-->
		<?= $this->Form->control('administrador', ['type' => 'text', 'label' => __('Administrador do fundo')]); ?>
		<?= $divResultados ?>
	</fieldset>
	<?= $this->Form->button(__('Aplicar Filtros')) ?>
	<?= $this->Form->end() ?>
</div>
<hr>

<div class="row">
	<h3><?= __('Fundos de Investimento Encontrados') ?></h3>
</div>
<div class="table-responsive">
	<div class="paginator">
		<ul class="pagination">
			<?= $this->Paginator->first('<< ' . __('first')) ?>
			<?= $this->Paginator->prev('< ' . __('previous')) ?>
			<?= $this->Paginator->numbers() ?>
			<?= $this->Paginator->next(__('next') . ' >') ?>
			<?= $this->Paginator->last(__('last') . ' >>') ?>
			&numsp;<?= $this->Paginator->counter(__('Página {{page}} de {{pages}}, mostrando {{current}} registro(s) do total de {{count}}')) ?>
		</ul>
	</div>
	<table>
		<thead>
			<tr>
				<!-- <th>Num</th> -->
				<!--th><?= $this->Paginator->sort('CNPJ') ?></th-->
				<th><?= $this->Paginator->sort('DENOM_SOCIAL', __('Nome do fundo')) ?></th>
				<!-- <th><?= $this->Paginator->sort('DT_REG_CVM', __('Data de registro')) ?></th> -->
				<th><?= $this->Paginator->sort('xxx', __('Administrador')) ?></th>

				<th><?= $this->Paginator->sort('xxx', __('Tipo / Anbima')) ?></th>
				<!--th><?= $this->Paginator->sort('xxx', __('Classe Anbima')) ?></th-->
				<th><?= $this->Paginator->sort('xxx', __('Aplicação mín')) ?></th>
				<th><?= $this->Paginator->sort('xxx', __('Cotização')) ?></th>
				<th><?= $this->Paginator->sort('xxx', __('Liquidação')) ?></th>
				<!--th><?= $this->Paginator->sort('xxx', __('Rentabilidade')) ?></th>
				<th><?= $this->Paginator->sort('xxx', __('Situação')) ?></th-->
				<th>+</th>
			</tr>
		</thead>
		<tbody>
			<!-- <?php $rowCount = ($this->Paginator->counter('{{page}}') - 1) * $this->Paginator->counter('{{current}}'); ?> -->
			<?php foreach ($cnpjFundos as $cnpjFundo): ?>
				<tr>
					<!-- <td><?= $this->Number->format( ++$rowCount) ?></td> -->
					<!--td></td-->
					<td>
						<?= $this->Html->link(h($cnpjFundo->DENOM_SOCIAL), ['action' => 'view', $cnpjFundo->id]) ?><br/>
						<small><?= h($cnpjFundo->CNPJ) ?></small>
						<!-- <a href=''><?= h($cnpjFundo->DENOM_SOCIAL) ?></a> -->
					</td>
					<!-- <td><?= h($cnpjFundo->DT_REG_CVM) ?></td> -->
					<td>
						<?= h($cnpjFundo->cadastro_fundos[0]->administrador_fundo->nome) ?>
					</td>
					<td>
						<?= h($cnpjFundo->cadastro_fundos[0]->tipo_classe_fundo->classe) ?><br/>
						<i>
							<?php
							if (count($cnpjFundo->doc_extratos_fundos) > 0) {
								echo(h($cnpjFundo->doc_extratos_fundos[0]->tipo_anbima_class->classe_anbima));
							} else {
								echo('-');
							}
							?>
						</i>
					</td>

					<!--td></td-->
					<?php
					if (count($cnpjFundo->doc_extratos_fundos) > 0) {
						echo('<td>' . $cnpjFundo->doc_extratos_fundos[0]->APLIC_MIN . '</td>');
						echo('<td>' . 'D+' . $cnpjFundo->doc_extratos_fundos[0]->QT_DIA_PAGTO_COTA . '</td>');
						echo('<td>' . 'D+' . $cnpjFundo->doc_extratos_fundos[0]->QT_DIA_PAGTO_RESGATE . '</td>');
					} else {
						echo('<td></td>');
						echo('<td></td>');
						echo('<td></td>');
					}
					?>
				<!--td><?= h($cnpjFundo->cadastro_fundos[0]->tipo_rentabilidade_fundo->rentabilidade) ?></td>
				<!--td><?= h($cnpjFundo->situacao_fundos[0]->tipo_situacao_fundo->situacao) ?></td-->
					<td><strong><a href="">+</a></strong></td>
				<!--div class="row">Mais informações</div-->
			</tr>
		<?php endforeach; ?>
		</tbody>
	</table>
</div>
<div class="paginator">
	<ul class="pagination">
		<?= $this->Paginator->first('<< ' . __('first')) ?>
		<?= $this->Paginator->prev('< ' . __('previous')) ?>
		<?= $this->Paginator->numbers() ?>
		<?= $this->Paginator->next(__('next') . ' >') ?>
		<?= $this->Paginator->last(__('last') . ' >>') ?>
	</ul>
	<p><?= $this->Paginator->counter(__('Página {{page}} de {{pages}}, mostrando {{current}} registro(s) do total de {{count}}')) ?></p>
</div>
<!--/div-->
