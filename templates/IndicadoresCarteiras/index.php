<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\IndicadoresCarteira[]|\Cake\Collection\CollectionInterface $indicadoresCarteiras
 */
?>
<div class="indicadoresCarteiras index content">
    <?= $this->Html->link(__('New Indicadores Carteira'), ['action' => 'add'], ['class' => 'button float-right']) ?>
    <h3><?= __('Indicadores Carteiras') ?></h3>
    <div class="table-responsive">
        <table>
            <thead>
                <tr>
                    <th><?= $this->Paginator->sort('carteiras_investimento_id') ?></th>
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
                <?php foreach ($indicadoresCarteiras as $indicadoresCarteira): ?>
                <tr>
                    <td><?= $indicadoresCarteira->has('carteiras_investimento') ? $this->Html->link($indicadoresCarteira->carteiras_investimento->id, ['controller' => 'CarteirasInvestimentos', 'action' => 'view', $indicadoresCarteira->carteiras_investimento->id]) : '' ?></td>
                    <td><?= $this->Number->format($indicadoresCarteira->periodo_meses) ?></td>
                    <td><?= h($indicadoresCarteira->data_final) ?></td>
                    <td><?= $this->Number->format($indicadoresCarteira->rentabilidade) ?></td>
                    <td><?= $this->Number->format($indicadoresCarteira->desvio_padrao) ?></td>
                    <td><?= $this->Number->format($indicadoresCarteira->num_valores) ?></td>
                    <td><?= $this->Number->format($indicadoresCarteira->rentab_min) ?></td>
                    <td><?= $this->Number->format($indicadoresCarteira->rentab_max) ?></td>
                    <td><?= $this->Number->format($indicadoresCarteira->max_drawdown) ?></td>
                    <td><?= $indicadoresCarteira->has('tipo_benchmark') ? $this->Html->link($indicadoresCarteira->tipo_benchmark->id, ['controller' => 'TipoBenchmarks', 'action' => 'view', $indicadoresCarteira->tipo_benchmark->id]) : '' ?></td>
                    <td><?= $this->Number->format($indicadoresCarteira->meses_acima_bench) ?></td>
                    <td><?= $this->Number->format($indicadoresCarteira->sharpe) ?></td>
                    <td><?= $this->Number->format($indicadoresCarteira->beta) ?></td>
                    <td class="actions">
                        <?= $this->Html->link(__('View'), ['action' => 'view', $indicadoresCarteira->carteiras_investimento_id]) ?>
                        <?= $this->Html->link(__('Edit'), ['action' => 'edit', $indicadoresCarteira->carteiras_investimento_id]) ?>
                        <?= $this->Form->postLink(__('Delete'), ['action' => 'delete', $indicadoresCarteira->carteiras_investimento_id], ['confirm' => __('Are you sure you want to delete # {0}?', $indicadoresCarteira->carteiras_investimento_id)]) ?>
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
