<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\TipoBenchmark[]|\Cake\Collection\CollectionInterface $tipoBenchmarks
 */
?>
<div class="tipoBenchmarks index content">
    <?= $this->Html->link(__('New Tipo Benchmark'), ['action' => 'add'], ['class' => 'button float-right']) ?>
    <h3><?= __('Tipo Benchmarks') ?></h3>
    <div class="table-responsive">
        <table>
            <thead>
                <tr>
                    <th><?= $this->Paginator->sort('id') ?></th>
                    <th><?= $this->Paginator->sort('nome') ?></th>
                    <th><?= $this->Paginator->sort('sigla') ?></th>
                    <th class="actions"><?= __('Actions') ?></th>
                </tr>
            </thead>
            <tbody>
                <?php foreach ($tipoBenchmarks as $tipoBenchmark): ?>
                <tr>
                    <td><?= $this->Number->format($tipoBenchmark->id) ?></td>
                    <td><?= h($tipoBenchmark->nome) ?></td>
                    <td><?= h($tipoBenchmark->sigla) ?></td>
                    <td class="actions">
                        <?= $this->Html->link(__('View'), ['action' => 'view', $tipoBenchmark->id]) ?>
                        <?= $this->Html->link(__('Edit'), ['action' => 'edit', $tipoBenchmark->id]) ?>
                        <?= $this->Form->postLink(__('Delete'), ['action' => 'delete', $tipoBenchmark->id], ['confirm' => __('Are you sure you want to delete # {0}?', $tipoBenchmark->id)]) ?>
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
