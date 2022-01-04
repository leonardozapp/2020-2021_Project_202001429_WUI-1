<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\Permissao $permissao
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Html->link(__('Edit Permissao'), ['action' => 'edit', $permissao->id], ['class' => 'side-nav-item']) ?>
            <?= $this->Form->postLink(__('Delete Permissao'), ['action' => 'delete', $permissao->id], ['confirm' => __('Are you sure you want to delete # {0}?', $permissao->id), 'class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('List Permissaos'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('New Permissao'), ['action' => 'add'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="permissaos view content">
            <h3><?= h($permissao->id) ?></h3>
            <table>
                <tr>
                    <th><?= __('Id') ?></th>
                    <td><?= $this->Number->format($permissao->id) ?></td>
                </tr>
                <tr>
                    <th><?= __('Supor Fck Root') ?></th>
                    <td><?= $permissao->supor_fck_root ? __('Yes') : __('No'); ?></td>
                </tr>
                <tr>
                    <th><?= __('Administrador Mng') ?></th>
                    <td><?= $permissao->administrador_mng ? __('Yes') : __('No'); ?></td>
                </tr>
                <tr>
                    <th><?= __('Carteiras Mng') ?></th>
                    <td><?= $permissao->carteiras_mng ? __('Yes') : __('No'); ?></td>
                </tr>
                <tr>
                    <th><?= __('Logs Mng') ?></th>
                    <td><?= $permissao->logs_mng ? __('Yes') : __('No'); ?></td>
                </tr>
                <tr>
                    <th><?= __('Operacoes Mng') ?></th>
                    <td><?= $permissao->operacoes_mng ? __('Yes') : __('No'); ?></td>
                </tr>
                <tr>
                    <th><?= __('Usuarios Mng') ?></th>
                    <td><?= $permissao->usuarios_mng ? __('Yes') : __('No'); ?></td>
                </tr>
                <tr>
                    <th><?= __('Tipos Mng') ?></th>
                    <td><?= $permissao->tipos_mng ? __('Yes') : __('No'); ?></td>
                </tr>
                <tr>
                    <th><?= __('Rel Mng') ?></th>
                    <td><?= $permissao->rel_mng ? __('Yes') : __('No'); ?></td>
                </tr>
            </table>
            <div class="related">
                <h4><?= __('Related Tipo Planos') ?></h4>
                <?php if (!empty($permissao->tipo_planos)) : ?>
                <div class="table-responsive">
                    <table>
                        <tr>
                            <th><?= __('Id') ?></th>
                            <th><?= __('Nome') ?></th>
                            <th><?= __('Descricao') ?></th>
                            <th><?= __('Permissao Id') ?></th>
                            <th class="actions"><?= __('Actions') ?></th>
                        </tr>
                        <?php foreach ($permissao->tipo_planos as $tipoPlanos) : ?>
                        <tr>
                            <td><?= h($tipoPlanos->id) ?></td>
                            <td><?= h($tipoPlanos->nome) ?></td>
                            <td><?= h($tipoPlanos->descricao) ?></td>
                            <td><?= h($tipoPlanos->permissao_id) ?></td>
                            <td class="actions">
                                <?= $this->Html->link(__('View'), ['controller' => 'TipoPlanos', 'action' => 'view', $tipoPlanos->id]) ?>
                                <?= $this->Html->link(__('Edit'), ['controller' => 'TipoPlanos', 'action' => 'edit', $tipoPlanos->id]) ?>
                                <?= $this->Form->postLink(__('Delete'), ['controller' => 'TipoPlanos', 'action' => 'delete', $tipoPlanos->id], ['confirm' => __('Are you sure you want to delete # {0}?', $tipoPlanos->id)]) ?>
                            </td>
                        </tr>
                        <?php endforeach; ?>
                    </table>
                </div>
                <?php endif; ?>
            </div>
        </div>
    </div>
</div>
