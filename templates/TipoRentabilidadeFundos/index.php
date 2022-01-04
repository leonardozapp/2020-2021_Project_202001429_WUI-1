<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\TipoRentabilidadeFundo[]|\Cake\Collection\CollectionInterface $tipoRentabilidadeFundos
 */
?>
<div class="tipoRentabilidadeFundos index content">
    <?= $this->Html->link(__('New Tipo Rentabilidade Fundo'), ['action' => 'add'], ['class' => 'button float-right']) ?>
    <h3><?= __('Tipo Rentabilidade Fundos') ?></h3>
    <div class="table-responsive">
        <table>
            <thead>
                <tr>
                    <th><?= $this->Paginator->sort('id') ?></th>
                    <th><?= $this->Paginator->sort('rentabilidade') ?></th>
                    <th class="actions"><?= __('Actions') ?></th>
                </tr>
            </thead>
            <tbody>
                <?php foreach ($tipoRentabilidadeFundos as $tipoRentabilidadeFundo): ?>
                <tr>
                    <td><?= $this->Number->format($tipoRentabilidadeFundo->id) ?></td>
                    <td><?= h($tipoRentabilidadeFundo->rentabilidade) ?></td>
                    <td class="actions">
                        <?= $this->Html->link(__('View'), ['action' => 'view', $tipoRentabilidadeFundo->id]) ?>
                        <?= $this->Html->link(__('Edit'), ['action' => 'edit', $tipoRentabilidadeFundo->id]) ?>
                        <?= $this->Form->postLink(__('Delete'), ['action' => 'delete', $tipoRentabilidadeFundo->id], ['confirm' => __('Are you sure you want to delete # {0}?', $tipoRentabilidadeFundo->id)]) ?>
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
        <p><?= $this->Paginator->counter(__('Página {{page}} de {{pages}}, mostrando {{current}} registro(s) do total de {{count}}')) ?></p>
    </div>
</div>
