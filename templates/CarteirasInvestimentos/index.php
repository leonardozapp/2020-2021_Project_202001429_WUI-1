<?php

/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\CarteirasInvestimento[]|\Cake\Collection\CollectionInterface $carteirasInvestimentos
 */
?>
<div class="carteirasInvestimentos index content">
    <?= $this->Html->link(__('Nova Carteira de Investimento'), ['action' => 'add'], ['class' => 'button float-right']) ?>
    <h3><?= __('Carteiras de Investimentos de {0}', $userName) ?></h3>
    <div class="table-responsive">
        <table>
            <thead>
                <tr>
                    <!--<th><?= $this->Paginator->sort('id') ?></th>-->
                    <!--<th><?= $this->Paginator->sort('usuario_id') ?></th>-->
                    <th><?= $this->Paginator->sort('nome') ?></th>
                    <th><?= $this->Paginator->sort('descricao') ?></th>
                    <th class="actions"><?= __('Actions') ?></th>
                </tr>
            </thead>
            <tbody>
                <?php foreach ($carteirasInvestimentos as $carteirasInvestimento) : ?>
                    <tr>
                        <!--<td><?= $this->Number->format($carteirasInvestimento->id) ?></td>-->
                        <!--<td><?= $carteirasInvestimento->has('usuario') ? $this->Html->link($carteirasInvestimento->usuario->nome, ['controller' => 'Usuarios', 'action' => 'view', $carteirasInvestimento->usuario->id]) : '' ?></td>-->
                        <td><?= h($carteirasInvestimento->nome) ?></td>
                        <td><?= h($carteirasInvestimento->descricao) ?></td>
                        <td class="actions">
                            <?= $this->Html->link(__('View'), ['action' => 'view', $carteirasInvestimento->id]) ?>
                            <?= $this->Html->link(__('Edit'), ['action' => 'edit', $carteirasInvestimento->id]) ?>
                            <?= $this->Form->postLink(__('Delete'), ['action' => 'delete', $carteirasInvestimento->id], ['confirm' => __('Are you sure you want to delete # {0}?', $carteirasInvestimento->id)]) ?>
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