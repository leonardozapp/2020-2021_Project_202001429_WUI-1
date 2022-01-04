<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\CadastroFundo[]|\Cake\Collection\CollectionInterface $cadastroFundos
 */
?>
<div class="cadastroFundos index content">
    <?= $this->Html->link(__('New Cadastro Fundo'), ['action' => 'add'], ['class' => 'button float-right']) ?>
    <h3><?= __('Cadastro Fundos') ?></h3>
    <div class="table-responsive">
        <table>
            <thead>
                <tr>
                    <th><?= $this->Paginator->sort('cnpj_fundo_id') ?></th>
                    <th><?= $this->Paginator->sort('DT_CONST') ?></th>
                    <th><?= $this->Paginator->sort('tipo_classe_fundo_id') ?></th>
                    <th><?= $this->Paginator->sort('DT_INI_CLASSE') ?></th>
                    <th><?= $this->Paginator->sort('tipo_rentabilidade_fundo_id') ?></th>
                    <th><?= $this->Paginator->sort('CONDOM_ABERTO') ?></th>
                    <th><?= $this->Paginator->sort('FUNDO_COTAS') ?></th>
                    <th><?= $this->Paginator->sort('FUNDO_EXCLUSIVO') ?></th>
                    <th><?= $this->Paginator->sort('INVEST_QUALIF') ?></th>
                    <th><?= $this->Paginator->sort('gestor_fundo_id') ?></th>
                    <th><?= $this->Paginator->sort('administrador_fundo_id') ?></th>
                    <th><?= $this->Paginator->sort('DT_REG_CVM') ?></th>
                    <th class="actions"><?= __('Actions') ?></th>
                </tr>
            </thead>
            <tbody>
                <?php foreach ($cadastroFundos as $cadastroFundo): ?>
                <tr>
                    <td><?= $cadastroFundo->has('cnpj_fundo') ? $this->Html->link($cadastroFundo->cnpj_fundo->id, ['controller' => 'CnpjFundos', 'action' => 'view', $cadastroFundo->cnpj_fundo->id]) : '' ?></td>
                    <td><?= h($cadastroFundo->DT_CONST) ?></td>
                    <td><?= $cadastroFundo->has('tipo_classe_fundo') ? $this->Html->link($cadastroFundo->tipo_classe_fundo->id, ['controller' => 'TipoClasseFundos', 'action' => 'view', $cadastroFundo->tipo_classe_fundo->id]) : '' ?></td>
                    <td><?= h($cadastroFundo->DT_INI_CLASSE) ?></td>
                    <td><?= $cadastroFundo->has('tipo_rentabilidade_fundo') ? $this->Html->link($cadastroFundo->tipo_rentabilidade_fundo->id, ['controller' => 'TipoRentabilidadeFundos', 'action' => 'view', $cadastroFundo->tipo_rentabilidade_fundo->id]) : '' ?></td>
                    <td><?= h($cadastroFundo->CONDOM_ABERTO) ?></td>
                    <td><?= h($cadastroFundo->FUNDO_COTAS) ?></td>
                    <td><?= h($cadastroFundo->FUNDO_EXCLUSIVO) ?></td>
                    <td><?= h($cadastroFundo->INVEST_QUALIF) ?></td>
                    <td><?= $cadastroFundo->has('gestor_fundo') ? $this->Html->link($cadastroFundo->gestor_fundo->id, ['controller' => 'GestorFundos', 'action' => 'view', $cadastroFundo->gestor_fundo->id]) : '' ?></td>
                    <td><?= $cadastroFundo->has('administrador_fundo') ? $this->Html->link($cadastroFundo->administrador_fundo->id, ['controller' => 'AdministradorFundos', 'action' => 'view', $cadastroFundo->administrador_fundo->id]) : '' ?></td>
                    <td><?= h($cadastroFundo->DT_REG_CVM) ?></td>
                    <td class="actions">
                        <?= $this->Html->link(__('View'), ['action' => 'view', $cadastroFundo->cnpj_fundo_id]) ?>
                        <?= $this->Html->link(__('Edit'), ['action' => 'edit', $cadastroFundo->cnpj_fundo_id]) ?>
                        <?= $this->Form->postLink(__('Delete'), ['action' => 'delete', $cadastroFundo->cnpj_fundo_id], ['confirm' => __('Are you sure you want to delete # {0}?', $cadastroFundo->cnpj_fundo_id)]) ?>
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
