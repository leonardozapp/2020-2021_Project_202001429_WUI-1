<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\RelBenchmarksClasseFundo[]|\Cake\Collection\CollectionInterface $relBenchmarksClasseFundos
 */
?>
<div class="relBenchmarksClasseFundos index content">
    <?= $this->Html->link(__('New Rel Benchmarks Classe Fundo'), ['action' => 'add'], ['class' => 'button float-right']) ?>
    <h3><?= __('Rel Benchmarks Classe Fundos') ?></h3>
    <div class="table-responsive">
        <table>
            <thead>
                <tr>
                    <th><?= $this->Paginator->sort('tipo_benchmark_id') ?></th>
                    <th><?= $this->Paginator->sort('tipo_classe_fundo_id') ?></th>
                    <th class="actions"><?= __('Actions') ?></th>
                </tr>
            </thead>
            <tbody>
                <?php foreach ($relBenchmarksClasseFundos as $relBenchmarksClasseFundo): ?>
                <tr>
                    <td><?= $relBenchmarksClasseFundo->has('tipo_benchmark') ? $this->Html->link($relBenchmarksClasseFundo->tipo_benchmark->id, ['controller' => 'TipoBenchmarks', 'action' => 'view', $relBenchmarksClasseFundo->tipo_benchmark->id]) : '' ?></td>
                    <td><?= $relBenchmarksClasseFundo->has('tipo_classe_fundo') ? $this->Html->link($relBenchmarksClasseFundo->tipo_classe_fundo->classe, ['controller' => 'TipoClasseFundos', 'action' => 'view', $relBenchmarksClasseFundo->tipo_classe_fundo->id]) : '' ?></td>
                    <td class="actions">
                        <?= $this->Html->link(__('View'), ['action' => 'view', $relBenchmarksClasseFundo->tipo_benchmark_id]) ?>
                        <?= $this->Html->link(__('Edit'), ['action' => 'edit', $relBenchmarksClasseFundo->tipo_benchmark_id]) ?>
                        <?= $this->Form->postLink(__('Delete'), ['action' => 'delete', $relBenchmarksClasseFundo->tipo_benchmark_id], ['confirm' => __('Are you sure you want to delete # {0}?', $relBenchmarksClasseFundo->tipo_benchmark_id)]) ?>
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
