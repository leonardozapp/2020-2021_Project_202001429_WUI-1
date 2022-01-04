<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\IndicadoresFundo[]|\Cake\Collection\CollectionInterface $indicadoresFundos
 */
?>
<div class="indicadoresFundos index content">
    <?= $this->Html->link(__('New Indicadores Fundo'), ['action' => 'add'], ['class' => 'button float-right']) ?>
    <h3><?= __('Indicadores Fundos') ?></h3>
    <div class="table-responsive">
        <table>
            <thead>
                <tr>
                    <th><?= $this->Paginator->sort('cnpj_fundo_id') ?></th>
                    <th><?= $this->Paginator->sort('periodo_meses') ?></th>
                    <th><?= $this->Paginator->sort('data_final') ?></th>
                    <th><?= $this->Paginator->sort('rentabilidade') ?></th>
                    <th><?= $this->Paginator->sort('desvio_padrao') ?></th>
                    <th><?= $this->Paginator->sort('num_valores') ?></th>
                    <th><?= $this->Paginator->sort('rentab_min') ?></th>
                    <th><?= $this->Paginator->sort('rentab_max') ?></th>
                    <th><?= $this->Paginator->sort('max_drawdown') ?></th>
                    <th><?= $this->Paginator->sort('tipo_benchmark_id') ?></th>
                    <th><?= $this->Paginator->sort('meses_acima_bench') ?></th>
                    <th><?= $this->Paginator->sort('sharpe') ?></th>
                    <th><?= $this->Paginator->sort('beta') ?></th>
                    <th class="actions"><?= __('Actions') ?></th>
                </tr>
            </thead>
            <tbody>
                <?php foreach ($indicadoresFundos as $indicadoresFundo): ?>
                <tr>
                    <td><?= $indicadoresFundo->has('cnpj_fundo') ? $this->Html->link($indicadoresFundo->cnpj_fundo->id, ['controller' => 'CnpjFundos', 'action' => 'view', $indicadoresFundo->cnpj_fundo->id]) : '' ?></td>
                    <td><?= $this->Number->format($indicadoresFundo->periodo_meses) ?></td>
                    <td><?= h($indicadoresFundo->data_final) ?></td>
                    <td><?= $this->Number->format($indicadoresFundo->rentabilidade) ?></td>
                    <td><?= $this->Number->format($indicadoresFundo->desvio_padrao) ?></td>
                    <td><?= $this->Number->format($indicadoresFundo->num_valores) ?></td>
                    <td><?= $this->Number->format($indicadoresFundo->rentab_min) ?></td>
                    <td><?= $this->Number->format($indicadoresFundo->rentab_max) ?></td>
                    <td><?= $this->Number->format($indicadoresFundo->max_drawdown) ?></td>
                    <td><?= $indicadoresFundo->has('tipo_benchmark') ? $this->Html->link($indicadoresFundo->tipo_benchmark->id, ['controller' => 'TipoBenchmarks', 'action' => 'view', $indicadoresFundo->tipo_benchmark->id]) : '' ?></td>
                    <td><?= $this->Number->format($indicadoresFundo->meses_acima_bench) ?></td>
                    <td><?= $this->Number->format($indicadoresFundo->sharpe) ?></td>
                    <td><?= $this->Number->format($indicadoresFundo->beta) ?></td>
                    <td class="actions">
                        <?= $this->Html->link(__('View'), ['action' => 'view', $indicadoresFundo->cnpj_fundo_id]) ?>
                        <?= $this->Html->link(__('Edit'), ['action' => 'edit', $indicadoresFundo->cnpj_fundo_id]) ?>
                        <?= $this->Form->postLink(__('Delete'), ['action' => 'delete', $indicadoresFundo->cnpj_fundo_id], ['confirm' => __('Are you sure you want to delete # {0}?', $indicadoresFundo->cnpj_fundo_id)]) ?>
                    </td>
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
        <p><?= $this->Paginator->counter(__('Page {{page}} of {{pages}}, showing {{current}} record(s) out of {{count}} total')) ?></p>
    </div>
</div>
