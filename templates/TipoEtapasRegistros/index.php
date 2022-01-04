<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\TipoEtapasRegistro[]|\Cake\Collection\CollectionInterface $tipoEtapasRegistros
 */
?>
<div class="tipoEtapasRegistros index content">
    <?= $this->Html->link(__('New Tipo Etapas Registro'), ['action' => 'add'], ['class' => 'button float-right']) ?>
    <h3><?= __('Tipo Etapas Registros') ?></h3>
    <div class="table-responsive">
        <table>
            <thead>
                <tr>
                    <th><?= $this->Paginator->sort('id') ?></th>
                    <th><?= $this->Paginator->sort('fase') ?></th>
                    <th><?= $this->Paginator->sort('etapa') ?></th>
                    <th><?= $this->Paginator->sort('podeLogar') ?></th>
                    <th><?= $this->Paginator->sort('podeInvestir') ?></th>
                    <th class="actions"><?= __('Actions') ?></th>
                </tr>
            </thead>
            <tbody>
                <?php foreach ($tipoEtapasRegistros as $tipoEtapasRegistro): ?>
                <tr>
                    <td><?= $this->Number->format($tipoEtapasRegistro->id) ?></td>
                    <td><?= $this->Number->format($tipoEtapasRegistro->fase) ?></td>
                    <td><?= h($tipoEtapasRegistro->etapa) ?></td>
                    <td><?= h($tipoEtapasRegistro->podeLogar) ?></td>
                    <td><?= h($tipoEtapasRegistro->podeInvestir) ?></td>
                    <td class="actions">
                        <?= $this->Html->link(__('View'), ['action' => 'view', $tipoEtapasRegistro->id]) ?>
                        <?= $this->Html->link(__('Edit'), ['action' => 'edit', $tipoEtapasRegistro->id]) ?>
                        <?= $this->Form->postLink(__('Delete'), ['action' => 'delete', $tipoEtapasRegistro->id], ['confirm' => __('Are you sure you want to delete # {0}?', $tipoEtapasRegistro->id)]) ?>
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
